using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Protobus;

/// <summary>
/// A MessageService with process lifecycle.
/// <code>
/// var ctx = new Context();
/// await ctx.InitAsync(Environment.GetEnvironmentVariable("AMQP_URL")!);
/// await RunnableService.StartAsync(ctx, c => new CalculatorService(c));
/// return await RunnableService.WaitForShutdownAsync();
/// </code>
/// <see cref="StartAsync{T}(Context, Func{Context, T})"/> initialises the service and registers it
/// for graceful shutdown, which runs on SIGINT or SIGTERM or on <see cref="RequestShutdown"/>:
/// every started service stops taking new work, in-flight work drains (up to
/// <see cref="Config.ShutdownDrainTimeoutMs"/>), each service's <see cref="CleanupAsync"/> runs, and
/// the contexts close. <see cref="CleanupAsync"/> never runs while a delivery is still being
/// handled, unless the drain deadline passed.
/// </summary>
public abstract class RunnableService : MessageService
{
    protected RunnableService(Context context, MessageServiceOptions? options = null) : base(context, options) { }

    /// <summary>Release the service's own resources at shutdown, once its work has drained. Default: nothing.</summary>
    protected virtual Task CleanupAsync() => Task.CompletedTask;

    private sealed record Started(Context Context, RunnableService Service);

    private static readonly object Sync = new();
    private static readonly List<Started> StartedServices = new();
    private static readonly List<PosixSignalRegistration> Signals = new();
    private static bool shuttingDown;
    private static TaskCompletionSource done = NewDone();
    private static volatile int exitCode;

    private static TaskCompletionSource NewDone() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Construct, initialise and register a service with default options.</summary>
    public static Task<T> StartAsync<T>(Context context, Func<Context, T> constructor) where T : RunnableService =>
        StartAsync(context, (c, _) => constructor(c), MessageServiceOptions.Default);

    /// <summary>
    /// Construct <c>constructor(context, options)</c>, initialise it, run
    /// <paramref name="postInit"/>, and register it for graceful shutdown. On a startup failure the
    /// service is stopped, the context closed, and the error rethrown.
    /// </summary>
    public static async Task<T> StartAsync<T>(Context context, Func<Context, MessageServiceOptions, T> constructor,
        MessageServiceOptions? options, Func<T, Task>? postInit = null) where T : RunnableService
    {
        InstallSignalHandlers();
        T? service = null;
        try
        {
            service = constructor(context, options ?? MessageServiceOptions.Default);
            Logger.Info("Starting service: " + service.ServiceName);
            await service.InitAsync().ConfigureAwait(false);
            if (postInit != null) await postInit(service).ConfigureAwait(false);
            lock (Sync)
            {
                if (shuttingDown) throw new NotReadyError("a shutdown is in progress");
                StartedServices.Add(new Started(context, service));
            }
            Logger.Info("Service ready: " + service.ServiceName);
            return service;
        }
        catch (Exception e)
        {
            Logger.Error("Service startup failed: " + e);
            if (service != null)
            {
                try
                {
                    await service.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception closeError)
                {
                    Logger.Debug("closing a service that failed to start: " + closeError.Message);
                }
            }
            await context.DisposeAsync().ConfigureAwait(false);
            exitCode = 1;
            throw;
        }
    }

    private static void InstallSignalHandlers()
    {
        lock (Sync)
        {
            if (Signals.Count > 0) return;
            foreach (var signal in new[] { PosixSignal.SIGINT, PosixSignal.SIGTERM })
            {
                try
                {
                    Signals.Add(PosixSignalRegistration.Create(signal, c =>
                    {
                        // The process stays up until the shutdown completes and the caller of
                        // WaitForShutdownAsync returns.
                        c.Cancel = true;
                        BeginShutdown("signal");
                    }));
                }
                catch (PlatformNotSupportedException)
                {
                    // Not every platform can intercept this signal; the others still shut down.
                }
            }
        }
    }

    /// <summary>Shut every started service down, as a signal would. Returns at once.</summary>
    public static void RequestShutdown() => BeginShutdown("requested");

    private static void BeginShutdown(string reason) => Connection.Background(() => ShutdownAsync(reason));

    /// <summary>Wait until a shutdown has completed; returns the exit code (0, or 1 after a startup failure).</summary>
    public static async Task<int> WaitForShutdownAsync()
    {
        Task t;
        lock (Sync) t = done.Task;
        await t.ConfigureAwait(false);
        return exitCode;
    }

    /// <summary>For tests that start services in-process: allow another start/shutdown cycle.</summary>
    internal static void ResetForTests()
    {
        lock (Sync)
        {
            StartedServices.Clear();
            shuttingDown = false;
            done = NewDone();
            exitCode = 0;
        }
    }

    private static async Task ShutdownAsync(string reason)
    {
        List<Started> services;
        TaskCompletionSource latch;
        lock (Sync)
        {
            if (shuttingDown) return;
            shuttingDown = true;
            services = new List<Started>(StartedServices);
            latch = done;
        }
        Logger.Info($"Shutdown initiated ({reason})");
        try
        {
            // 1. Stop taking new work, keeping channels open: cleanup must not run while
            //    consumers still deliver.
            foreach (var s in services)
            {
                try
                {
                    await s.Service.StopConsumingAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Error($"Failed to stop consumers of {s.Service.ServiceName}: {e}");
                }
            }
            Logger.Info("Stopped accepting new messages");
            // 2. Let work in hand finish, including the reply, retry or DLQ publish that settles it.
            var budget = Config.ShutdownDrainTimeoutMs;
            var drained = new HashSet<Connection>();
            foreach (var s in services)
            {
                var c = s.Context.Connection;
                if (!drained.Add(c)) continue;
                var inFlight = c.InFlightDeliveries;
                if (inFlight == 0) continue;
                Logger.Info($"Draining {inFlight} in-flight message(s), up to {budget}ms");
                Logger.Info(await c.DrainInFlightAsync(budget).ConfigureAwait(false)
                    ? "In-flight messages drained"
                    : $"Drain deadline reached with {c.InFlightDeliveries} still running; they stay unacknowledged "
                        + "and will be redelivered");
            }
            // 3. Only now is it safe to release the services' own resources.
            foreach (var s in services)
            {
                try
                {
                    await s.Service.CleanupAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Error($"Service cleanup failed for {s.Service.ServiceName}: {e}");
                }
            }
            var closed = new HashSet<Context>();
            foreach (var s in services)
            {
                if (!closed.Add(s.Context)) continue;
                try
                {
                    await s.Context.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Error("Connection close failed: " + e);
                }
            }
            Logger.Info("Connection closed");
        }
        finally
        {
            latch.TrySetResult();
        }
    }
}

/// <summary>
/// A service that also holds a proxy to its own contract, for calling other replicas of itself.
/// <code>
/// class Worker : ProxiedService&lt;WorkerProtobus.Proxy&gt;
/// {
///     protected override WorkerProtobus.Proxy NewProxy(Context ctx, string name) => new(ctx, name);
///     ...
/// }
/// </code>
/// </summary>
public abstract class ProxiedService<TProxy> : RunnableService where TProxy : class
{
    private volatile TProxy? proxy;

    protected ProxiedService(Context context, MessageServiceOptions? options = null) : base(context, options) { }

    /// <summary>Build the proxy; <see cref="InitAsync"/> initialises it through <see cref="InitProxy"/>.</summary>
    protected abstract TProxy NewProxy(Context context, string serviceName);

    /// <summary>Initialise the proxy built by <see cref="NewProxy"/>. Generated proxies initialise themselves.</summary>
    protected virtual void InitProxy(TProxy p)
    {
        if (p is ServiceProxy sp && !sp.IsInitialized) sp.Init();
    }

    public TProxy? Proxy => proxy;

    public override async Task InitAsync()
    {
        await base.InitAsync().ConfigureAwait(false);
        var p = NewProxy(Context, ServiceName);
        InitProxy(p);
        proxy = p;
    }
}
