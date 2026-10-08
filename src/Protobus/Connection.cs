using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Protobus.Amqp;
using Protobus.Internal;

namespace Protobus;

/// <summary>Reconnection backoff.</summary>
/// <param name="MaxRetries">Consecutive failed attempts before giving up; 0 retries forever.</param>
/// <param name="InitialDelayMs">The first delay; each later one is multiplied, up to the maximum, with up to 30% jitter.</param>
/// <param name="MaxDelayMs">The longest delay.</param>
/// <param name="BackoffMultiplier">The factor between delays.</param>
public sealed record ReconnectionOptions(int MaxRetries = 10, long InitialDelayMs = 1000, long MaxDelayMs = 30000,
    double BackoffMultiplier = 2);

/// <summary>Extra context handed to a message handler.</summary>
public sealed class MessageHandlerContext
{
    internal MessageHandlerContext(CancellationToken cancellation, string routingKey, string? messageId,
        bool redelivered, IDictionary<string, object?> headers)
    {
        CancellationToken = cancellation;
        RoutingKey = routingKey;
        MessageId = messageId;
        Redelivered = redelivered;
        Headers = headers;
    }

    /// <summary>
    /// Fires when the processing timeout elapses or, for a streaming reply, when the caller
    /// cancels. A handler is never aborted, so one doing long work should observe it.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>The routing key the broker delivered on.</summary>
    public string RoutingKey { get; }

    /// <summary>Stable across every redelivery and retry hop: deduplicate on it. Null only if the publisher set none.</summary>
    public string? MessageId { get; }

    /// <summary>The broker has delivered this message before.</summary>
    public bool Redelivered { get; }

    /// <summary>The delivery's headers; never null.</summary>
    public IDictionary<string, object?> Headers { get; }
}

/// <summary>
/// What a handler returns: nothing (no reply), one encoded reply, or a stream of encoded chunks,
/// each published with <c>x-protobus-final=false</c> and the last with <c>true</c>.
/// </summary>
public sealed class HandlerResult
{
    internal readonly byte[]? Reply;
    internal readonly IAsyncEnumerable<byte[]>? Stream;

    private HandlerResult(byte[]? reply, IAsyncEnumerable<byte[]>? stream)
    {
        Reply = reply;
        Stream = stream;
    }

    public static readonly HandlerResult None = new(null, null);

    public static HandlerResult FromReply(byte[] reply) => new(reply, null);

    public static HandlerResult FromStream(IAsyncEnumerable<byte[]> chunks) => new(null, chunks);
}

/// <summary>Handles one delivery.</summary>
public delegate Task<HandlerResult> MessageHandler(byte[] content, string correlationId, MessageHandlerContext context);

/// <summary>
/// An error carrying the reply its caller should receive on a terminal path (dead-lettered, or
/// rejected without retry). MessageService throws one for an unhandled handler error,
/// pre-encoded and sanitised; the connection settles on the inner exception.
/// </summary>
public sealed class ErrorWithReply : Exception
{
    public ErrorWithReply(Exception cause, byte[] reply) : base("protobus: handler failed", cause) => Reply = reply;

    public byte[] Reply { get; }
}

/// <summary>How a consumer consumes.</summary>
public sealed class ConsumeOptions
{
    public string ConsumerTag { get; init; } = "";
    public bool NoAck { get; init; }
    public bool Exclusive { get; init; }

    /// <summary>
    /// Handle deliveries one at a time, in arrival order, on the client's thread rather than in
    /// parallel. Replies use it: a stream's chunks must be seen in the order they arrived. The
    /// handler must complete synchronously.
    /// </summary>
    public bool Ordered { get; init; }

    /// <summary>Handlers this consumer runs at once; 0 leaves it to the prefetch, which bounds a late-ack consumer.</summary>
    public int MaxConcurrency { get; init; }

    /// <summary>Builds the caller's reply for a failure that carries none, such as a processing timeout.</summary>
    public Func<byte[], Exception, byte[]?>? BuildErrorReply { get; init; }

    /// <summary>Called when the broker cancels the consumer.</summary>
    public Action? OnCancelled { get; init; }
}

/// <summary>Retry for a consumer.</summary>
/// <param name="RetryExchangeName">
/// The topic exchange the retry queue is bound to with <c>#</c>. A retry is published here under
/// the original routing key, which is what lets the post-TTL dead-letter hop route it back.
/// </param>
public sealed record ConsumeRetryOptions(int MaxRetries, string RetryQueueName, string RetryExchangeName, string DlqName,
    Func<Exception, bool> IsHandledError);

/// <summary>What to publish with.</summary>
public sealed record PublishOptions(MessageProperties Properties, bool Mandatory = false);

/// <summary>
/// The broker connection every protobus component of a context shares: reconnection with
/// coordinated restoration, confirmed publishing, and the consume loop that runs a handler and
/// settles its delivery (reply, ack, retry, dead-letter).
/// </summary>
public sealed class Connection
{
    private readonly ITransport transport;
    private readonly object stateLock = new();
    private string url = "";
    private ReconnectionOptions reconnection = new();
    private IAmqpConnection? handle;
    private bool connected;
    private bool reconnecting;
    private bool ready;
    private bool manualDisconnect;
    private bool shutDown;
    private long generation;
    private int reconnectAttempts;
    private CancellationTokenSource? reconnectTimer;
    private string? abandoned;
    private readonly List<TaskCompletionSource<bool>> readyWaiters = new();
    private readonly SemaphoreSlim connectMutex = new(1, 1);
    private readonly List<Func<long, Task>> restorers = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<DeliveryEntry, byte>> activeDeliveries = new();

    private readonly object drainLock = new();
    private long inFlight;
    private long running;
    private readonly List<TaskCompletionSource<bool>> drainWaiters = new();

    public Connection(ITransport? transport = null) => this.transport = transport ?? new RabbitTransport();

    /// <summary>The connection is going to reconnect: attempt number and delay in ms.</summary>
    public event Action<int, long>? Reconnecting;

    /// <summary>The connection came back and every component is restored.</summary>
    public event Action? Reconnected;

    /// <summary>
    /// The connection went down: lost (a reconnection follows) or closed by
    /// <see cref="DisconnectAsync"/>. Runs on the client's thread or the disconnecting caller's.
    /// </summary>
    public event Action? Disconnected;

    /// <summary>A connection-level failure, such as giving up reconnecting.</summary>
    public event Action<Exception>? Error;

    public bool IsConnected { get { lock (stateLock) return connected; } }
    public bool IsReconnecting { get { lock (stateLock) return reconnecting; } }

    /// <summary>The socket is up AND every restorer has finished: what a publisher wants.</summary>
    public bool IsReady { get { lock (stateLock) return ready; } }

    // ---- timers and background work -----------------------------------------------------

    /// <summary>
    /// Run <paramref name="action"/> once after <paramref name="delay"/>, on the thread pool; dispose
    /// the result to cancel it. A disposed timer never runs its action, even one already due.
    /// </summary>
    internal IDisposable Schedule(TimeSpan delay, Action action) => new ScheduledAction(delay, action);

    private sealed class ScheduledAction : IDisposable
    {
        private readonly Action action;
        private readonly Timer timer;
        /// <summary>0 while pending; 1 once it has run or been cancelled, whichever came first.</summary>
        private int done;

        internal ScheduledAction(TimeSpan delay, Action action)
        {
            this.action = action;
            // The callback holds this, and the runtime's timer queue holds the callback, so a
            // caller may drop the handle without the timer being collected before it fires.
            timer = new Timer(_ => Fire(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            timer.Change(delay < TimeSpan.Zero ? TimeSpan.Zero : delay, Timeout.InfiniteTimeSpan);
        }

        private void Fire()
        {
            if (Interlocked.Exchange(ref done, 1) != 0) return;
            timer.Dispose();
            try
            {
                action();
            }
            catch (Exception e)
            {
                Logger.Error("a timer task failed: " + e);
            }
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref done, 1);
            timer.Dispose();
        }
    }

    internal static void Background(Func<Task> work)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await work().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error("background work failed: " + e);
            }
        });
    }

    private static void Safely(Action? listener)
    {
        if (listener == null) return;
        foreach (Action l in listener.GetInvocationList())
        {
            try
            {
                l();
            }
            catch (Exception e)
            {
                Logger.Error("connection listener failed: " + e);
            }
        }
    }

    // ---- restorers and readiness -----------------------------------------------------------

    /// <summary>
    /// Register topology to restore on reconnection. Restorers run in registration order, and the
    /// connection reports itself reconnected only once all have finished; one that throws fails
    /// the attempt, which is retried. Dispose the result to unregister.
    /// </summary>
    public IDisposable RegisterRestorer(Func<long, Task> restorer)
    {
        Func<long, Task> entry = g => restorer(g);
        lock (restorers) restorers.Add(entry);
        return new Unregister(() => { lock (restorers) restorers.Remove(entry); });
    }

    private sealed class Unregister : IDisposable
    {
        private Action? action;

        internal Unregister(Action action) => this.action = action;

        public void Dispose() => Interlocked.Exchange(ref action, null)?.Invoke();
    }

    /// <summary>Wait until the connection carries traffic.</summary>
    /// <exception cref="NotReadyError">when it is closed or gives up first, or after the timeout</exception>
    public Task WhenReadyAsync(long? timeoutMs = null)
    {
        var limit = timeoutMs ?? Config.ConnectionReadyTimeoutMs;
        TaskCompletionSource<bool> waiter;
        lock (stateLock)
        {
            if (ready) return Task.CompletedTask;
            if (manualDisconnect) return Task.FromException(new NotReadyError("the connection has been closed"));
            if (abandoned != null) return Task.FromException(new NotReadyError(abandoned));
            waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            readyWaiters.Add(waiter);
        }
        var timer = Schedule(TimeSpan.FromMilliseconds(limit), () =>
        {
            lock (stateLock) readyWaiters.Remove(waiter);
            waiter.TrySetException(new NotReadyError($"the connection did not become ready within {limit}ms"));
        });
        waiter.Task.ContinueWith(_ => timer.Dispose(), TaskScheduler.Default);
        return waiter.Task;
    }

    private static void Release(List<TaskCompletionSource<bool>> waiters, Exception? error)
    {
        foreach (var w in waiters)
        {
            if (error == null) w.TrySetResult(true);
            else w.TrySetException(error);
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------------

    /// <summary>Connect.</summary>
    /// <exception cref="AlreadyConnectedError">when connected</exception>
    /// <exception cref="AmqpException">when the broker cannot be reached</exception>
    public async Task ConnectAsync(string url, ReconnectionOptions? options = null)
    {
        lock (stateLock)
        {
            if (connected) throw new AlreadyConnectedError();
            this.url = url;
            reconnection = options ?? new ReconnectionOptions();
            manualDisconnect = false;
            abandoned = null;
        }
        await DoConnectAsync().ConfigureAwait(false);
        // Nothing to restore on a first connect: components initialise against it, so the
        // socket coming up is readiness, unless it already went again.
        List<TaskCompletionSource<bool>> waiters;
        lock (stateLock)
        {
            if (!connected) return;
            reconnecting = false;
            ready = true;
            waiters = new List<TaskCompletionSource<bool>>(readyWaiters);
            readyWaiters.Clear();
        }
        Release(waiters, null);
    }

    private async Task DoConnectAsync()
    {
        await connectMutex.WaitAsync().ConfigureAwait(false);
        try
        {
            long gen;
            string target;
            lock (stateLock)
            {
                gen = generation;
                target = url;
            }
            Logger.Info("connecting to bus - " + Logger.RedactUrl(target));
            IAmqpConnection h;
            try
            {
                h = await transport.ConnectAsync(target, (int)Config.HeartbeatSeconds).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error("failed to connect: " + Errors.MessageOf(e));
                lock (stateLock) connected = false;
                throw;
            }
            bool stale;
            lock (stateLock)
            {
                stale = gen != generation || manualDisconnect;
                if (!stale)
                {
                    handle = h;
                    connected = true;
                    reconnectAttempts = 0;
                    // `reconnecting` stays set: on the reconnect path restoration still has
                    // to run, and clearing it would let a socket dying mid-restore schedule a
                    // second reconnection alongside the one its failure schedules.
                }
            }
            if (stale)
            {
                Logger.Info("discarding a connection that completed after disconnect");
                await h.CloseAsync().ConfigureAwait(false);
                throw new ReconnectionError("connection was torn down while connecting");
            }
            h.OnClose(reason => OnHandleClosed(h, reason));
            Logger.Info("connected to message bus");
        }
        finally
        {
            connectMutex.Release();
        }
    }

    private void OnHandleClosed(IAmqpConnection h, string? reason)
    {
        lock (stateLock)
        {
            if (h != handle) return;
            if (manualDisconnect)
            {
                Logger.Info("connection closed (manual disconnect)");
                return;
            }
            connected = false;
            ready = false;
            // Retire this generation: a restoration in flight against it is now working on a
            // dead socket, and its next generation check aborts it.
            generation++;
        }
        Logger.Warn("connection closed unexpectedly" + (reason == null ? "" : ": " + reason));
        Safely(Disconnected);
        ScheduleReconnect();
    }

    private void ScheduleReconnect()
    {
        int attempt;
        long delay;
        string? giveUp = null;
        lock (stateLock)
        {
            if (manualDisconnect || reconnecting || shutDown) return;
            if (reconnection.MaxRetries > 0 && reconnectAttempts >= reconnection.MaxRetries)
            {
                giveUp = $"max reconnection attempts ({reconnection.MaxRetries}) exceeded";
                abandoned = giveUp;
                attempt = 0;
                delay = 0;
            }
            else
            {
                reconnecting = true;
                attempt = ++reconnectAttempts;
                var @base = Math.Min(reconnection.InitialDelayMs * Math.Pow(reconnection.BackoffMultiplier, attempt - 1),
                    reconnection.MaxDelayMs);
                delay = (long)Math.Floor(@base + Random.Shared.NextDouble() * 0.3 * @base);
                var cts = new CancellationTokenSource();
                reconnectTimer = cts;
                _ = Task.Delay(TimeSpan.FromMilliseconds(delay), cts.Token).ContinueWith(t =>
                {
                    if (!t.IsCanceled) Background(ReconnectAttemptAsync);
                }, TaskScheduler.Default);
            }
        }
        if (giveUp != null)
        {
            Logger.Error(giveUp);
            List<TaskCompletionSource<bool>> waiters;
            lock (stateLock)
            {
                ready = false;
                waiters = new List<TaskCompletionSource<bool>>(readyWaiters);
                readyWaiters.Clear();
            }
            Release(waiters, new NotReadyError(giveUp));
            var error = new ReconnectionError(giveUp);
            if (Error != null)
            {
                foreach (Action<Exception> l in Error.GetInvocationList())
                {
                    try { l(error); } catch (Exception e) { Logger.Error("connection listener failed: " + e); }
                }
            }
            return;
        }
        Logger.Info($"scheduling reconnection attempt {attempt} in {delay}ms");
        if (Reconnecting != null)
        {
            foreach (Action<int, long> l in Reconnecting.GetInvocationList())
            {
                try { l(attempt, delay); } catch (Exception e) { Logger.Error("connection listener failed: " + e); }
            }
        }
    }

    private async Task ReconnectAttemptAsync()
    {
        int attempt;
        lock (stateLock)
        {
            reconnectTimer = null;
            // Read before connecting: a successful connect resets it.
            attempt = reconnectAttempts;
            if (manualDisconnect)
            {
                reconnecting = false;
                return;
            }
        }
        try
        {
            await DoConnectAsync().ConfigureAwait(false);
            long gen;
            lock (stateLock) gen = generation;
            // Restoration is part of reconnecting: until every component has its channel,
            // queues and consumers back, the socket is up but cannot be used.
            await RunRestorersAsync(gen).ConfigureAwait(false);
            List<TaskCompletionSource<bool>> waiters;
            lock (stateLock)
            {
                if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
                reconnecting = false;
                ready = true;
                waiters = new List<TaskCompletionSource<bool>>(readyWaiters);
                readyWaiters.Clear();
            }
            Release(waiters, null);
            Logger.Info($"reconnection successful after {attempt} attempts");
            Safely(Reconnected);
        }
        catch (Exception e)
        {
            lock (stateLock)
            {
                if (manualDisconnect)
                {
                    reconnecting = false;
                    return;
                }
            }
            Logger.Error($"reconnection attempt {attempt} failed: {Errors.MessageOf(e)}");
            // A generation that connected but could not be restored looks healthy and serves
            // nothing. Drop it and let the backoff try again.
            await DiscardGenerationAsync().ConfigureAwait(false);
            lock (stateLock)
            {
                reconnectAttempts = attempt;
                reconnecting = false;
            }
            ScheduleReconnect();
        }
    }

    private async Task RunRestorersAsync(long gen)
    {
        List<Func<long, Task>> snapshot;
        lock (restorers) snapshot = new List<Func<long, Task>>(restorers);
        foreach (var r in snapshot)
        {
            lock (stateLock)
                if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
            await r(gen).ConfigureAwait(false);
        }
        lock (stateLock)
            if (gen != generation) throw new ReconnectionError("connection was torn down while restoring");
    }

    private async Task DiscardGenerationAsync()
    {
        IAmqpConnection? h;
        lock (stateLock)
        {
            generation++;
            ready = false;
            connected = false;
            h = handle;
            handle = null;
        }
        if (h != null)
        {
            try
            {
                await h.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug("failed closing an unrestorable connection: " + e.Message);
            }
        }
    }

    internal long Generation { get { lock (stateLock) return generation; } }

    /// <summary>
    /// Close deliberately. Waiters on readiness fail with NotReadyError, and the
    /// <see cref="Disconnected"/> listeners run first, so pending calls and streams fail at once.
    /// </summary>
    public async Task DisconnectAsync()
    {
        IAmqpConnection? h;
        List<TaskCompletionSource<bool>> waiters;
        lock (stateLock)
        {
            manualDisconnect = true;
            generation++;
            reconnectTimer?.Cancel();
            reconnectTimer = null;
            reconnecting = false;
            ready = false;
            waiters = new List<TaskCompletionSource<bool>>(readyWaiters);
            readyWaiters.Clear();
            h = handle;
            handle = null;
            connected = false;
        }
        Release(waiters, new NotReadyError("the connection has been closed"));
        // Listeners first, so pending calls and streams fail as disconnected before closing the
        // socket can fail a queued write under them.
        Safely(Disconnected);
        if (h != null)
        {
            try
            {
                await h.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug("closing the connection: " + e.Message);
            }
            Logger.Info("connection closed (manual disconnect)");
        }
    }

    internal void MarkShutDown()
    {
        lock (stateLock) shutDown = true;
    }

    // ---- streams and draining -----------------------------------------------------------------

    private sealed class DeliveryEntry
    {
        internal readonly CancellationTokenSource Cancellation = new();
        internal volatile bool Cancelled;
    }

    /// <summary>
    /// Stop producing a streaming reply the caller has abandoned: fire the handler's cancellation
    /// token and publish nothing more it produces. Cooperative.
    /// </summary>
    /// <returns>whether a matching in-flight delivery was found</returns>
    public bool CancelStream(string correlationId)
    {
        if (!activeDeliveries.TryGetValue(correlationId, out var entries) || entries.IsEmpty) return false;
        // A redelivery can overlap its predecessor; both are the caller's stream.
        foreach (var e in entries.Keys)
        {
            e.Cancelled = true;
            try
            {
                e.Cancellation.Cancel();
            }
            catch (Exception ex)
            {
                Logger.Debug("a cancellation callback failed: " + ex.Message);
            }
        }
        Logger.Debug($"stream {correlationId} cancelled by the caller");
        return true;
    }

    /// <summary>Messages being handled, counting handlers still running after a processing timeout settled them.</summary>
    public long InFlightDeliveries { get { lock (drainLock) return Math.Max(inFlight, running); } }

    /// <summary>Wait for in-flight work to finish; false when the deadline passed with work still running.</summary>
    public async Task<bool> DrainInFlightAsync(long timeoutMs)
    {
        TaskCompletionSource<bool> waiter;
        lock (drainLock)
        {
            if (Math.Max(inFlight, running) == 0) return true;
            waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            drainWaiters.Add(waiter);
        }
        var done = await Task.WhenAny(waiter.Task, Task.Delay(TimeSpan.FromMilliseconds(timeoutMs))).ConfigureAwait(false);
        lock (drainLock) drainWaiters.Remove(waiter);
        return done == waiter.Task || InFlightDeliveries == 0;
    }

    private void DeliveryStarted()
    {
        lock (drainLock) inFlight++;
    }

    private void DeliveryFinished()
    {
        lock (drainLock)
        {
            if (inFlight > 0) inFlight--;
            WakeDrain();
        }
    }

    private void HandlerStarted()
    {
        lock (drainLock) running++;
    }

    private void HandlerFinished()
    {
        lock (drainLock)
        {
            if (running > 0) running--;
            WakeDrain();
        }
    }

    private void WakeDrain()
    {
        if (Math.Max(inFlight, running) > 0) return;
        foreach (var w in drainWaiters) w.TrySetResult(true);
        drainWaiters.Clear();
    }

    // ---- channel operations ------------------------------------------------------------------

    public Task<IAmqpChannel> OpenChannelAsync()
    {
        IAmqpConnection? h;
        lock (stateLock) h = handle;
        if (h == null) throw new NotConnectedError("no broker connection");
        return h.OpenChannelAsync();
    }

    private static readonly IDictionary<string, object?> NoArguments = new Dictionary<string, object?>();

    public Task DeclareExchangeAsync(IAmqpChannel channel, string exchange, string type) =>
        channel.DeclareExchangeAsync(exchange, type, true, false, false, NoArguments);

    public Task<string> DeclareQueueAsync(IAmqpChannel channel, string queue, bool durable, bool exclusive,
        bool autoDelete, IDictionary<string, object?>? arguments = null) =>
        channel.DeclareQueueAsync(queue, durable, exclusive, autoDelete, arguments ?? NoArguments);

    // ---- consuming -----------------------------------------------------------------------------

    /// <summary>
    /// The properties a retry or dead-letter republish copies from the original. Not carried:
    /// deliveryMode (persistent at every hop), expiration (it would race the retry queue's TTL,
    /// and on the DLQ delete the evidence), userId (validated against the publishing
    /// connection's user).
    /// </summary>
    internal static MessageProperties Carried(MessageProperties p) => new()
    {
        ContentType = p.ContentType,
        ContentEncoding = p.ContentEncoding,
        Priority = p.Priority,
        Timestamp = p.Timestamp,
        Type = p.Type,
        AppId = p.AppId,
    };

    private sealed class InFlight
    {
        internal readonly IAmqpChannel Channel;
        internal readonly string Queue;
        internal readonly Delivery Delivery;
        internal readonly ConsumeOptions Options;
        internal readonly bool LateAck;
        internal readonly ConsumeRetryOptions? Retry;
        internal readonly DeliveryEntry Entry = new();
        internal readonly string CorrelationId;
        internal readonly string? ReplyTo;
        internal readonly IDictionary<string, object?> Headers;
        private int finished;

        internal InFlight(IAmqpChannel channel, string queue, Delivery delivery, ConsumeOptions options, bool lateAck,
            ConsumeRetryOptions? retry)
        {
            Channel = channel;
            Queue = queue;
            Delivery = delivery;
            Options = options;
            LateAck = lateAck;
            Retry = retry;
            CorrelationId = delivery.Properties.CorrelationId ?? "";
            ReplyTo = delivery.Properties.ReplyTo;
            Headers = delivery.Properties.Headers ?? new Dictionary<string, object?>();
        }

        internal bool SettlesLate => !Options.NoAck && LateAck;

        internal bool Finish() => Interlocked.Exchange(ref finished, 1) == 0;
    }

    /// <summary>Runs at most a limit of tasks at once; the rest wait, in order.</summary>
    private sealed class Limited
    {
        private readonly int limit;
        private readonly Queue<Func<Task>> waiting = new();
        private int active;

        internal Limited(int limit) => this.limit = limit;

        internal void Run(Func<Task> task)
        {
            lock (waiting)
            {
                if (active >= limit)
                {
                    waiting.Enqueue(task);
                    return;
                }
                active++;
            }
            Background(() => Work(task));
        }

        private async Task Work(Func<Task> first)
        {
            var current = first;
            while (current != null)
            {
                try
                {
                    await current().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Error("a delivery task failed unexpectedly: " + e);
                }
                lock (waiting)
                {
                    if (!waiting.TryDequeue(out current)) active--;
                }
            }
        }
    }

    /// <summary>
    /// Consume <paramref name="queue"/> with <paramref name="handler"/>. An early-ack consumer
    /// acknowledges on delivery and never retries. A late-ack consumer settles after the handler:
    /// it replies, then acks; on a failure it retries, dead-letters once the retries are spent, and
    /// answers the caller on every terminal path.
    /// </summary>
    /// <returns>the consumer tag</returns>
    public Task<string> ConsumeAsync(IAmqpChannel channel, string queue, MessageHandler handler, ConsumeOptions options,
        bool lateAck, ConsumeRetryOptions? retry, long? processingTimeoutMs)
    {
        var limited = options.MaxConcurrency > 0 ? new Limited(options.MaxConcurrency) : null;
        return channel.ConsumeAsync(queue, options.ConsumerTag, options.NoAck, options.Exclusive, delivery =>
        {
            // Counted for the whole settle, so a graceful shutdown waits for the reply, retry or
            // dead-letter publish and not just the handler body.
            DeliveryStarted();
            var d = new InFlight(channel, queue, delivery, options, lateAck, retry);
            // Ordered (replies): the client delivers the next only once this one is handled.
            if (options.Ordered) return HandleDeliveryAsync(d, handler, processingTimeoutMs);
            if (limited != null) limited.Run(() => HandleDeliveryAsync(d, handler, processingTimeoutMs));
            else Background(() => HandleDeliveryAsync(d, handler, processingTimeoutMs));
            return Task.CompletedTask;
        }, () =>
        {
            Logger.Warn($"consumer for {queue} was cancelled by the broker");
            try
            {
                options.OnCancelled?.Invoke();
            }
            catch (Exception e)
            {
                Logger.Error("consumer-cancelled listener failed: " + e);
            }
        });
    }

    private static long RetryCount(IDictionary<string, object?> headers)
    {
        var n = Headers.Integer(Headers.Get(headers, "x-retry-count"));
        return n is null or < 0 ? 0 : n.Value;
    }

    private async Task HandleDeliveryAsync(InFlight d, MessageHandler handler, long? processingTimeoutMs)
    {
        var retryCount = RetryCount(d.Headers);
        Logger.Debug($"incoming message on {d.Queue} (routing key {d.Delivery.RoutingKey})"
            + (retryCount > 0 ? $" (retry {retryCount})" : ""));

        if (!d.Options.NoAck && !d.LateAck)
        {
            try
            {
                await d.Channel.AckAsync(d.Delivery.DeliveryTag).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug($"early ack failed on {d.Queue}: {e.Message}");
            }
        }

        // Registered for the whole delivery, so CancelStream() reaches the handler at any point.
        activeDeliveries.GetOrAdd(d.CorrelationId, _ => new ConcurrentDictionary<DeliveryEntry, byte>())[d.Entry] = 0;

        var limit = processingTimeoutMs ?? Config.MessageProcessingTimeout;
        var context = new MessageHandlerContext(d.Entry.Cancellation.Token, d.Delivery.RoutingKey,
            d.Delivery.Properties.MessageId, d.Delivery.Redelivered, d.Headers);

        HandlerStarted();
        if (d.Options.Ordered)
        {
            // Reply routing: inline, to keep the order, and completing at once; no processing
            // timeout applies.
            Task<HandlerResult> routed;
            try
            {
                routed = handler(d.Delivery.Body, d.CorrelationId, context);
            }
            catch (Exception e)
            {
                routed = Task.FromException<HandlerResult>(e);
            }
            _ = routed.ContinueWith(_ => HandlerFinished(), TaskScheduler.Default);
            await Settle(d, routed).ConfigureAwait(false);
            return;
        }

        // The deadline starts first and the handler runs on the thread pool, so work it does
        // before its first await (or a handler that never returns) is bounded too.
        using var expiry = new CancellationTokenSource();
        var deadline = Task.Delay(TimeSpan.FromMilliseconds(limit), expiry.Token);
        var run = Task.Run(() => handler(d.Delivery.Body, d.CorrelationId, context));
        _ = run.ContinueWith(_ => HandlerFinished(), TaskScheduler.Default);
        var first = await Task.WhenAny(run, deadline).ConfigureAwait(false);
        if (first == deadline && !run.IsCompleted)
        {
            // The handler runs on, but its result is discarded: the attempt has failed.
            try
            {
                d.Entry.Cancellation.Cancel();
            }
            catch (Exception e)
            {
                Logger.Debug("a cancellation callback failed: " + e.Message);
            }
            await SettleErrorAsync(d, new TimeoutError($"message {d.CorrelationId} exceeded the {limit}ms processing timeout"))
                .ConfigureAwait(false);
            return;
        }
        expiry.Cancel();
        await Settle(d, run).ConfigureAwait(false);
    }

    private async Task Settle(InFlight d, Task<HandlerResult> run)
    {
        HandlerResult result;
        try
        {
            result = await run.ConfigureAwait(false) ?? HandlerResult.None;
        }
        catch (Exception e)
        {
            await SettleErrorAsync(d, Errors.Unwrap(e)).ConfigureAwait(false);
            return;
        }
        try
        {
            // The reply goes before the settlement, so the worst case is a redelivered request
            // rather than a settled one whose reply was lost.
            if (!string.IsNullOrEmpty(d.ReplyTo))
            {
                if (result.Stream != null)
                    await PublishStreamReplyAsync(d.Channel, d.ReplyTo, d.CorrelationId, result.Stream, d.Entry).ConfigureAwait(false);
                else if (result.Reply != null)
                    await PublishAsync(d.Channel, Config.CallbacksExchangeName, d.ReplyTo, result.Reply,
                        new PublishOptions(ReplyProperties(d.CorrelationId, null))).ConfigureAwait(false);
            }
            if (d.SettlesLate) await d.Channel.AckAsync(d.Delivery.DeliveryTag).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await SettleErrorAsync(d, Errors.Unwrap(e)).ConfigureAwait(false);
            return;
        }
        FinishDelivery(d);
    }

    private void FinishDelivery(InFlight d)
    {
        if (!d.Finish()) return;
        if (activeDeliveries.TryGetValue(d.CorrelationId, out var group))
        {
            group.TryRemove(d.Entry, out _);
            if (group.IsEmpty) activeDeliveries.TryRemove(new KeyValuePair<string, ConcurrentDictionary<DeliveryEntry, byte>>(d.CorrelationId, group));
        }
        DeliveryFinished();
    }

    private static MessageProperties ReplyProperties(string correlationId, IDictionary<string, object?>? headers) => new()
    {
        ContentType = "application/octet-stream",
        CorrelationId = correlationId,
        Headers = headers,
    };

    private async Task SettleErrorAsync(InFlight d, Exception error)
    {
        try
        {
            await SettleErrorOrThrowAsync(d, error).ConfigureAwait(false);
        }
        catch (Exception t)
        {
            // Every path that can throw runs before the settlement, so the message is still
            // unacknowledged. Hand it back to the broker, after a pause so a persistent failure
            // does not spin.
            Logger.Error($"failed to settle message on {d.Queue}: {Errors.MessageOf(t)}"
                + (d.SettlesLate ? ". Requeueing it in 1 s." : ""));
            if (d.SettlesLate)
            {
                Schedule(TimeSpan.FromSeconds(1), () => Background(async () =>
                {
                    try
                    {
                        await d.Channel.RejectAsync(d.Delivery.DeliveryTag, true).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        Logger.Debug("requeue failed (the channel is likely gone, which requeues it anyway): " + e.Message);
                    }
                }));
            }
        }
        finally
        {
            FinishDelivery(d);
        }
    }

    private async Task SettleErrorOrThrowAsync(InFlight d, Exception error)
    {
        // A cancelled delivery is a normal outcome: the caller asked to stop.
        if (d.Entry.Cancelled)
        {
            Logger.Debug($"message {d.CorrelationId} ended because its stream was cancelled");
            if (d.SettlesLate) await d.Channel.AckAsync(d.Delivery.DeliveryTag).ConfigureAwait(false);
            return;
        }

        var cause = error;
        byte[]? reply = null;
        if (error is ErrorWithReply ewr)
        {
            cause = ewr.InnerException ?? error;
            reply = ewr.Reply;
        }
        else if (d.Options.BuildErrorReply != null)
        {
            try
            {
                reply = d.Options.BuildErrorReply(d.Delivery.Body, error);
            }
            catch (Exception e)
            {
                Logger.Debug("could not build an error reply: " + e.Message);
            }
        }
        Logger.Error($"unhandled error consuming bus message - {Errors.MessageOf(cause)}\n{cause}");

        // BEST EFFORT, deliberately: the reply may be lost (the caller has a timeout), while the
        // dead-letter queue is the only durable record of the message.
        async Task PublishErrorReply()
        {
            if (string.IsNullOrEmpty(d.ReplyTo) || reply == null) return;
            try
            {
                await PublishAsync(d.Channel, Config.CallbacksExchangeName, d.ReplyTo, reply,
                    new PublishOptions(ReplyProperties(d.CorrelationId, null))).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error($"failed to publish the error reply for {d.CorrelationId} to {d.ReplyTo}: "
                    + $"{Errors.MessageOf(e)}. The caller will time out; settling the message anyway.");
            }
        }

        if (!d.SettlesLate)
        {
            // Acked before processing: retry and dead-lettering are impossible, but the caller
            // must still be told.
            await PublishErrorReply().ConfigureAwait(false);
            return;
        }

        var retry = d.Retry;
        var handled = retry != null && retry.IsHandledError(cause);
        var original = d.Delivery.Properties;
        var retryCount = RetryCount(d.Headers);
        // The delivered routing key, which the retry hop preserves. The header is written for
        // operators; nothing routes by it.
        var originalRoutingKey = d.Delivery.RoutingKey;
        var firstFailure = Headers.Get(d.Headers, "x-first-failure-time") ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (retry != null && !handled && retry.MaxRetries > 0)
        {
            if (retryCount < retry.MaxRetries)
            {
                var next = retryCount + 1;
                Logger.Warn($"retrying message {d.CorrelationId} (attempt {next}/{retry.MaxRetries})");
                var headers = new Dictionary<string, object?>(d.Headers)
                {
                    ["x-retry-count"] = next,
                    ["x-original-routing-key"] = originalRoutingKey,
                    ["x-first-failure-time"] = firstFailure,
                    ["x-last-error"] = Errors.SafeErrorSummary(cause),
                };
                var props = Carried(original) with
                {
                    DeliveryMode = 2, CorrelationId = d.CorrelationId, MessageId = original.MessageId,
                    ReplyTo = d.ReplyTo, Headers = headers,
                };
                // Published to the retry EXCHANGE under the original routing key, so the post-TTL
                // dead-letter hop routes it back to the service queue. The caller stays parked.
                if (!string.IsNullOrEmpty(retry.RetryExchangeName))
                    await PublishAsync(d.Channel, retry.RetryExchangeName, originalRoutingKey, d.Delivery.Body,
                        new PublishOptions(props, true)).ConfigureAwait(false);
                else
                    await PublishAsync(d.Channel, "", retry.RetryQueueName, d.Delivery.Body, new PublishOptions(props, true))
                        .ConfigureAwait(false);
                await d.Channel.AckAsync(d.Delivery.DeliveryTag).ConfigureAwait(false);
            }
            else
            {
                Logger.Error($"message {d.CorrelationId} exceeded max retries ({retry.MaxRetries}), sending to DLQ");
                await PublishErrorReply().ConfigureAwait(false);
                var headers = new Dictionary<string, object?>(d.Headers)
                {
                    ["x-retry-count"] = retryCount,
                    ["x-original-routing-key"] = originalRoutingKey,
                    ["x-original-queue"] = d.Queue,
                    ["x-first-failure-time"] = firstFailure,
                    ["x-dlq-time"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ["x-last-error"] = Errors.SafeErrorSummary(cause),
                };
                var props = Carried(original) with
                {
                    DeliveryMode = 2, CorrelationId = d.CorrelationId, MessageId = original.MessageId, Headers = headers,
                };
                await PublishAsync(d.Channel, "", retry.DlqName, d.Delivery.Body, new PublishOptions(props, true))
                    .ConfigureAwait(false);
                await d.Channel.AckAsync(d.Delivery.DeliveryTag).ConfigureAwait(false);
            }
        }
        else
        {
            if (handled) Logger.Warn($"handled error for message {d.CorrelationId}, not retrying: {Errors.MessageOf(cause)}");
            await PublishErrorReply().ConfigureAwait(false);
            Logger.Warn($"rejecting message {d.CorrelationId}");
            await d.Channel.RejectAsync(d.Delivery.DeliveryTag, false).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Publish a streaming reply: every chunk carries the correlationId and an
    /// <c>x-protobus-seq</c> from 0; all but the last <c>x-protobus-final=false</c>, the last
    /// <c>true</c>. A producer that yields nothing produces one empty final chunk. Look-ahead by
    /// one: a chunk is held until the next exists or the producer ends.
    /// </summary>
    private async Task PublishStreamReplyAsync(IAmqpChannel channel, string replyTo, string correlationId,
        IAsyncEnumerable<byte[]> chunks, DeliveryEntry entry)
    {
        Task PublishOne(byte[] body, long seq, bool last) => PublishAsync(channel, Config.CallbacksExchangeName, replyTo, body,
            new PublishOptions(ReplyProperties(correlationId, new Dictionary<string, object?>
            {
                [Config.HeaderFinal] = last,
                [Config.HeaderSeq] = seq,
            })));

        byte[]? buffered = null;
        long seq = 0;
        await using (var e = chunks.GetAsyncEnumerator(entry.Cancellation.Token))
        {
            while (true)
            {
                bool more;
                try
                {
                    more = await e.MoveNextAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (entry.Cancelled)
                {
                    throw new StreamCancelledException();
                }
                if (!more) break;
                // A caller that cancelled is not listening: stop sending, and stop the producer
                // (disposing the enumerator runs its finally blocks).
                if (entry.Cancelled)
                {
                    Logger.Debug($"stream {correlationId} cancelled after {seq} chunk(s)");
                    throw new StreamCancelledException();
                }
                if (buffered != null)
                {
                    await PublishOne(buffered, seq, false).ConfigureAwait(false);
                    seq++;
                }
                buffered = e.Current ?? Array.Empty<byte>();
            }
        }
        if (entry.Cancelled) throw new StreamCancelledException();
        if (buffered != null) await PublishOne(buffered, seq, true).ConfigureAwait(false);
        else await PublishOne(Array.Empty<byte>(), 0, true).ConfigureAwait(false);
    }

    /// <summary>Raised inside a stream's production once its caller has cancelled.</summary>
    public sealed class StreamCancelledException : ProtobusException
    {
        public StreamCancelledException() : base("the stream was cancelled by its caller") { }
    }

    // ---- publishing ------------------------------------------------------------------------------

    /// <summary>Per-channel publish bookkeeping.</summary>
    private sealed class PublishState
    {
        internal readonly object Lock = new();
        /// <summary>Writes waiting for the channel, in publish order, as (write, unwritten).</summary>
        internal readonly Queue<(Func<Task> Write, Action Unwritten)> Writes = new();
        internal bool Writing;
        internal readonly Dictionary<string, int> Awaiting = new();
        /// <summary>Publishes holding a slot: written, or queued for the writer.</summary>
        internal int InFlight;
        /// <summary>Publishes waiting for a slot, as (resume, notSent). A waiting publish holds no slot.</summary>
        internal readonly LinkedList<(Action Resume, Action NotSent)> Waiters = new();
        /// <summary>The channel has closed: nothing more is queued, and nothing waits.</summary>
        internal bool Closed;
    }

    private readonly ConditionalWeakTable<IAmqpChannel, PublishState> publishStates = new();

    private PublishState PublishStateFor(IAmqpChannel channel)
    {
        var created = false;
        PublishState state;
        lock (publishStates)
        {
            if (!publishStates.TryGetValue(channel, out state!))
            {
                state = new PublishState();
                publishStates.Add(channel, state);
                created = true;
            }
        }
        if (created)
        {
            // Everything not yet written fails here, directly, whatever the writer is doing: it
            // may be blocked in a write only this close will end. A publish waiting for a slot is
            // failed, not resumed, and holds no slot to release.
            channel.OnClose(_ =>
            {
                List<(Action, Action)> parked;
                List<(Func<Task>, Action)> unwritten;
                lock (state.Lock)
                {
                    state.Closed = true;
                    parked = new List<(Action, Action)>(state.Waiters);
                    state.Waiters.Clear();
                    unwritten = new List<(Func<Task>, Action)>(state.Writes);
                    state.Writes.Clear();
                }
                foreach (var (_, notWritten) in unwritten) notWritten();
                foreach (var (_, notSent) in parked) notSent();
            });
        }
        return state;
    }

    /// <summary>
    /// Publish and complete only once RabbitMQ has confirmed the message: positively and, for a
    /// mandatory publish, routed. Everything else fails with a <see cref="PublishError"/>;
    /// <see cref="PublishConfirmTimeoutError"/> and <see cref="ChannelClosedError"/> are
    /// AMBIGUOUS, which is why every publish carries a messageId (a UUID unless one is set).
    /// </summary>
    /// <returns>the messageId</returns>
    public Task<string> PublishAsync(IAmqpChannel? channel, string exchange, string routingKey, byte[] content,
        PublishOptions options)
    {
        var props = options.Properties;
        var messageId = string.IsNullOrEmpty(props.MessageId) ? Guid.NewGuid().ToString() : props.MessageId;
        var describe = (string.IsNullOrEmpty(exchange) ? "(default)" : exchange) + " -> " + routingKey;
        var result = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (channel == null || !channel.IsOpen)
        {
            result.SetException(new ChannelClosedError(describe + ": the channel is closed", messageId));
            return result.Task;
        }
        var state = PublishStateFor(channel);
        Func<Task> send = () => SendAsync(channel, state, exchange, routingKey, content, props, options.Mandatory, messageId,
            describe, result);
        // A queued write whose channel closed first: never sent; its slot is released exactly
        // once, here or by the write itself, whichever dequeues it.
        Action unwritten = () =>
        {
            result.TrySetException(new ChannelClosedError(describe + " was not written: the channel closed first", messageId));
            ReleaseSlot(state);
        };
        // The caller's deadline runs from here, so waiting for a slot counts against it too.
        var confirmTimeout = Config.PublishConfirmTimeoutMs;
        LinkedListNode<(Action, Action)>? parked = null;
        var deadline = Schedule(TimeSpan.FromMilliseconds(confirmTimeout), () =>
        {
            bool neverSent;
            lock (state.Lock)
            {
                neverSent = parked != null && parked.List != null;
                if (neverSent) state.Waiters.Remove(parked!);
            }
            result.TrySetException(new PublishConfirmTimeoutError(neverSent
                ? $"{describe} waited {confirmTimeout}ms for one of the channel's {Config.MaxOutstandingConfirms} confirm slots and was not sent"
                : $"no broker confirm for {describe} within {confirmTimeout}ms", messageId));
        });
        result.Task.ContinueWith(_ => deadline.Dispose(), TaskScheduler.Default);
        bool now;
        lock (state.Lock)
        {
            if (state.Closed)
            {
                result.TrySetException(new ChannelClosedError(describe + ": the channel is closed", messageId));
                return result.Task;
            }
            now = state.InFlight < Config.MaxOutstandingConfirms;
            if (now)
            {
                state.InFlight++;
            }
            else
            {
                Action resume = () =>
                {
                    if (result.Task.IsCompleted)
                    {
                        // Timed out while waiting: hand the slot straight on.
                        ReleaseSlot(state);
                        return;
                    }
                    Write(state, send, unwritten);
                };
                Action notSent = () => result.TrySetException(new ChannelClosedError(
                    describe + " was not sent: the channel closed while it waited for a confirm slot", messageId));
                parked = state.Waiters.AddLast((resume, notSent));
            }
        }
        if (now) Write(state, send, unwritten);
        return result.Task;
    }

    /// <summary>
    /// Hand a write to the channel's writer: one at a time per channel, in order, on the thread
    /// pool, never on the caller's or a timer's thread. A socket write can block, and a blocked
    /// caller could not see its deadline pass.
    /// </summary>
    private void Write(PublishState state, Func<Task> task, Action unwritten)
    {
        bool start, closed;
        lock (state.Lock)
        {
            closed = state.Closed;
            if (!closed) state.Writes.Enqueue((task, unwritten));
            start = !closed && !state.Writing;
            if (start) state.Writing = true;
        }
        if (closed)
        {
            unwritten();
            return;
        }
        if (start) Background(() => DrainWritesAsync(state));
    }

    private static async Task DrainWritesAsync(PublishState state)
    {
        while (true)
        {
            (Func<Task> Write, Action Unwritten) next;
            lock (state.Lock)
            {
                if (!state.Writes.TryDequeue(out next))
                {
                    state.Writing = false;
                    return;
                }
            }
            try
            {
                await next.Write().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error("a publish failed unexpectedly: " + e);
            }
        }
    }

    /// <summary>A slot came free: give it to the next waiting publish, which then owns it.</summary>
    private static void ReleaseSlot(PublishState state)
    {
        Action? next = null;
        lock (state.Lock)
        {
            if (state.Waiters.First != null)
            {
                next = state.Waiters.First.Value.Resume;
                state.Waiters.RemoveFirst();
            }
            else
            {
                state.InFlight--;
            }
        }
        next?.Invoke();
    }

    /// <summary>
    /// Send one publish holding a slot. The slot is held until the broker answers or the channel
    /// closes, not until the caller stops waiting: a timed-out publish may still be stored, and
    /// the bound is on what the broker has not answered.
    /// </summary>
    private static async Task SendAsync(IAmqpChannel channel, PublishState state, string exchange, string routingKey,
        byte[] content, MessageProperties props, bool mandatory, string messageId, string describe,
        TaskCompletionSource<string> result)
    {
        if (result.Task.IsCompleted)
        {
            // Its deadline passed while it waited for the writer: never sent.
            ReleaseSlot(state);
            return;
        }
        var final = props with { MessageId = messageId };
        lock (state.Lock)
        {
            // A return is matched to its publish by messageId. When two mandatory publishes
            // sharing one are pending on this channel, a per-publish tag tells their returns apart.
            if (mandatory && state.Awaiting.TryGetValue(messageId, out var n) && n > 0)
            {
                var headers = new Dictionary<string, object?>(props.Headers ?? new Dictionary<string, object?>())
                {
                    [RabbitTransport.PublishTagHeader] = Guid.NewGuid().ToString(),
                };
                final = final with { Headers = headers };
            }
            state.Awaiting[messageId] = state.Awaiting.TryGetValue(messageId, out var c) ? c + 1 : 1;
        }
        var settled = 0;
        void SettleOnce(Exception? error)
        {
            if (Interlocked.Exchange(ref settled, 1) != 0) return;
            lock (state.Lock)
            {
                if (state.Awaiting.TryGetValue(messageId, out var c)) state.Awaiting[messageId] = c - 1;
                if (state.Awaiting.TryGetValue(messageId, out var left) && left <= 0) state.Awaiting.Remove(messageId);
            }
            ReleaseSlot(state);
            // A caller whose deadline already passed keeps that answer.
            if (error == null) result.TrySetResult(messageId);
            else result.TrySetException(error);
        }
        try
        {
            await channel.PublishAsync(exchange, routingKey, content, final, mandatory, (outcome, detail) =>
            {
                switch (outcome)
                {
                    case ConfirmOutcome.Ack:
                        SettleOnce(null);
                        break;
                    case ConfirmOutcome.Nack:
                        SettleOnce(new PublishNackedError($"broker nacked {describe}" + (string.IsNullOrEmpty(detail) ? "" : ": " + detail), messageId));
                        break;
                    case ConfirmOutcome.Returned:
                        SettleOnce(new UnroutableError($"{describe} was confirmed but returned as unroutable", messageId));
                        break;
                    default:
                        SettleOnce(new ChannelClosedError($"{describe} was unconfirmed when the channel closed"
                            + (string.IsNullOrEmpty(detail) ? "" : $" ({detail})"), messageId));
                        break;
                }
            }).ConfigureAwait(false);
        }
        catch (AmqpException e)
        {
            SettleOnce(new ChannelClosedError($"{describe} could not be written: {e.Message}", messageId));
        }
        catch (Exception e)
        {
            SettleOnce(e);
        }
    }
}
