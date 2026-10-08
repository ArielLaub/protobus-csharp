using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Pbtest;
using Protobus.Amqp;
using Protobus.Internal;
using Protobus.Testing;
using Xunit;


namespace Protobus.Tests;

/// <summary>A bus on the in-memory broker: a broker, a context and helpers to run services and proxies on it.</summary>
public abstract class MemoryBus : IAsyncLifetime
{
    protected MemoryBroker Broker = null!;
    protected Context Ctx = null!;
    private readonly List<MessageService> services = new();
    private readonly List<Context> contexts = new();

    public virtual async ValueTask InitializeAsync()
    {
        Config.Reset();
        Config.Set("RPC_CALL_TIMEOUT_MS", "10000");
        Config.Set("STREAM_IDLE_TIMEOUT_MS", "10000");
        Config.Set("PUBLISH_CONFIRM_TIMEOUT_MS", "5000");
        Config.Set("SHUTDOWN_DRAIN_TIMEOUT_MS", "2000");
        Logger.Level = Environment.GetEnvironmentVariable("PROTOBUS_TEST_LOG") == null ? LogLevel.Silent : LogLevel.Debug;
        RunnableService.ResetForTests();
        Broker = new MemoryBroker();
        Ctx = await NewContextAsync();
    }

    public virtual async ValueTask DisposeAsync()
    {
        foreach (var s in services)
        {
            try
            {
                await s.DisposeAsync();
            }
            catch (Exception)
            {
                // Some tests close their own.
            }
        }
        foreach (var c in contexts) await c.DisposeAsync();
        await Broker.DisposeAsync();
        Config.Reset();
        Logger.Level = LogLevel.Info;
        GC.SuppressFinalize(this);
    }

    protected static ReconnectionOptions FastReconnect() => new(MaxRetries: 50, InitialDelayMs: 10, MaxDelayMs: 50);

    protected Task<Context> NewContextAsync() => NewContextAsync(new ContextOptions { Reconnection = FastReconnect() });

    protected async Task<Context> NewContextAsync(ContextOptions options)
    {
        var c = new Context(options with { Transport = Broker });
        await c.InitAsync("amqp://guest:guest@memory/");
        contexts.Add(c);
        return c;
    }

    protected Task<CalcService> ServeAsync(MessageServiceOptions? options = null) =>
        ServeAsync((c, o) => new CalcService(c, o), options, Ctx);

    protected async Task<T> ServeAsync<T>(Func<Context, MessageServiceOptions?, T> make, MessageServiceOptions? options,
        Context on) where T : MessageService
    {
        var s = make(on, options);
        await s.InitAsync();
        services.Add(s);
        return s;
    }

    protected CalcProtobus.Proxy Proxy(string name = CalcProtobus.ServiceFullName) => Proxy(Ctx, name);

    protected static CalcProtobus.Proxy Proxy(Context on, string name = CalcProtobus.ServiceFullName)
    {
        var p = new CalcProtobus.Proxy(on, name);
        p.Init();
        return p;
    }

    protected static Task<bool> Eventually(Func<bool> predicate, TimeSpan? timeout = null) =>
        MemoryBroker.WaitForAsync(predicate, timeout ?? TimeSpan.FromSeconds(5));

    /// <summary>A header's value as text, or "" when absent.</summary>
    protected static string Header(Delivery d, string name) => Headers.Text(Headers.Get(d.Properties.Headers, name)) ?? "";

    protected static AddRequest Add(int a, int b) => new() { A = a, B = b };
}
