using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Protobus.Amqp;
using Protobus.Internal;

namespace Protobus;

/// <summary>
/// A queue and its consumer: declared on <see cref="InitAsync"/>, consuming from
/// <see cref="StartAsync"/>, and restored after a reconnection (or a channel lost on a live
/// connection) without the owner doing anything.
/// </summary>
public abstract class BaseListener
{
    protected readonly Connection Connection;

    protected string ExchangeName = "";
    protected string ExchangeType = "";
    protected bool LateAck;
    /// <summary>Prefetch for late-ack consumers; null falls back to <see cref="Config.DefaultPrefetch"/>.</summary>
    protected int? MaxConcurrent;
    protected long? MessageTtlMs;
    protected int? MaxPriority;
    protected long? ProcessingTimeoutMs;
    /// <summary>Handle deliveries in order; see <see cref="ConsumeOptions.Ordered"/>.</summary>
    protected bool OrderedDelivery;
    /// <summary>Builds the caller's reply for a failure that carries none (a processing timeout).</summary>
    protected Func<byte[], Exception, byte[]?>? BuildErrorReply;

    private readonly object sync = new();
    /// <summary>Serialises restoration against start and stop: a listener is never consuming twice.</summary>
    private readonly SemaphoreSlim restoreLock = new(1, 1);
    private IAmqpChannel? channel;
    private string queueName = "";
    private string configuredQueueName = "";
    private bool isAnonymous = true;
    private string consumerTag = "";
    private MessageHandler? handler;
    private readonly List<string> bindings = new();
    private bool initialized;
    private bool wasStarted;
    private bool closing;
    private bool consumerLost;
    private bool rebuildScheduled;
    private IAmqpChannel? rebuildFor;
    private int rebuildFailures;
    private IDisposable? detachRestorer;
    private Action? onQueueReplaced;

    protected BaseListener(Connection connection)
    {
        Connection = connection;
        AttachRestorer();
        connection.Disconnected += OnDisconnected;
    }

    /// <summary>Run <paramref name="listener"/> when a restoration declared the queue under a new name.</summary>
    public void OnQueueReplaced(Action? listener) => onQueueReplaced = listener;

    protected virtual string ListenerName => GetType().Name;

    /// <summary>Payload size and correlation id only: never the body.</summary>
    protected virtual Task<HandlerResult> DefaultHandler(byte[] message, string correlationId, MessageHandlerContext context)
    {
        Logger.Warn($"unhandled message by default handler ({message.Length} bytes, correlationId "
            + $"{(string.IsNullOrEmpty(correlationId) ? "none" : correlationId)})");
        return Task.FromResult(HandlerResult.None);
    }

    public bool IsInitialized { get { lock (sync) return initialized; } }
    protected IAmqpChannel? Channel { get { lock (sync) return channel; } }
    public string QueueName { get { lock (sync) return queueName; } }
    protected string ConfiguredQueueName { get { lock (sync) return configuredQueueName; } }
    protected bool IsAnonymous { get { lock (sync) return isAnonymous; } }

    private void AttachRestorer()
    {
        lock (sync)
        {
            if (detachRestorer != null) return;
            detachRestorer = Connection.RegisterRestorer(_ => RestoreAsync());
        }
    }

    private void DetachRestorer()
    {
        IDisposable? d;
        lock (sync)
        {
            d = detachRestorer;
            detachRestorer = null;
        }
        d?.Dispose();
    }

    /// <summary>The connection was lost: the channel went with it.</summary>
    protected virtual void OnDisconnected()
    {
        lock (sync)
        {
            channel = null;
            consumerTag = "";
        }
    }

    /// <summary>
    /// Put this listener's channel, queue, bindings and consumer back. A failure propagates to the
    /// connection, which treats the whole generation as unusable and retries.
    /// </summary>
    protected async Task RestoreAsync()
    {
        await restoreLock.WaitAsync().ConfigureAwait(false);
        try
        {
            bool started;
            List<string> keys;
            IAmqpChannel? previous;
            string previousQueue;
            lock (sync)
            {
                if (!initialized || closing) return;
                started = wasStarted;
                keys = new List<string>(bindings);
                previous = channel;
                previousQueue = queueName;
            }
            Logger.Info($"{ListenerName}: reconnected, re-initializing...");
            await ReinitializeAsync().ConfigureAwait(false);
            var ch = Channel!;
            var queue = QueueName;
            if (queue != previousQueue)
            {
                try
                {
                    onQueueReplaced?.Invoke();
                }
                catch (Exception e)
                {
                    Logger.Error($"{ListenerName}: queue-replaced listener failed: {e}");
                }
            }
            foreach (var key in keys) await ch.BindQueueAsync(queue, ExchangeName, key).ConfigureAwait(false);
            await RestoreTopologyAsync().ConfigureAwait(false);
            lock (sync)
            {
                started = started && wasStarted && !closing;
                consumerLost = false;
            }
            if (started) await StartConsumingAsync().ConfigureAwait(false);
            // The channel this replaces can still be open (one whose consumer the broker
            // cancelled); closed only now, so its close is not mistaken for a loss.
            if (previous != null && previous != ch && previous.IsOpen) await previous.CloseAsync().ConfigureAwait(false);
            Logger.Info($"{ListenerName}: successfully re-initialized after reconnection");
        }
        finally
        {
            restoreLock.Release();
        }
    }

    /// <summary>Redeclare whatever a subclass declares beyond the main queue.</summary>
    protected virtual Task RestoreTopologyAsync() => Task.CompletedTask;

    private async Task ReinitializeAsync()
    {
        var ch = await Connection.OpenChannelAsync().ConfigureAwait(false);
        if (LateAck) await ch.PrefetchAsync(EffectivePrefetch()).ConfigureAwait(false);
        await Connection.DeclareExchangeAsync(ch, ExchangeName, ExchangeType).ConfigureAwait(false);
        var anonymous = IsAnonymous;
        // An anonymous queue is gone with the old connection: declare a new one.
        var name = await Connection.DeclareQueueAsync(ch, anonymous ? "" : ConfiguredQueueName, !anonymous, anonymous,
            anonymous, BuildQueueArguments()).ConfigureAwait(false);
        if (ExchangeType == "direct") await ch.BindQueueAsync(name, ExchangeName, name).ConfigureAwait(false);
        lock (sync)
        {
            channel = ch;
            queueName = name;
        }
        WatchChannel(ch);
    }

    /// <summary>
    /// A channel can close while the connection stays up. The listener is rebuilt then, as on a
    /// reconnection; otherwise it would go quiet behind a healthy connection.
    /// </summary>
    private void WatchChannel(IAmqpChannel ch)
    {
        ch.OnClose(reason =>
        {
            lock (sync)
                if (closing || !initialized || channel != ch) return;
            if (!Connection.IsReady) return; // the reconnection restores it
            ScheduleRebuild($"channel closed on a live connection ({reason})");
        });
    }

    /// <summary>One rebuild pending at a time, backing off while they keep failing.</summary>
    private void ScheduleRebuild(string reason)
    {
        int failures;
        lock (sync)
        {
            if (rebuildScheduled || closing || !initialized) return;
            rebuildScheduled = true;
            rebuildFor = channel;
            failures = rebuildFailures;
        }
        var delay = Math.Min(100L << Math.Min(failures, 9), 30000);
        Logger.Warn($"{ListenerName}: {reason}; rebuilding it in {delay}ms");
        Connection.Schedule(TimeSpan.FromMilliseconds(delay), () => Connection.Background(RebuildAsync));
    }

    private async Task RebuildAsync()
    {
        lock (sync)
        {
            rebuildScheduled = false;
            if (closing || !initialized) return;
            if (consumerLost && !wasStarted)
            {
                consumerLost = false;
                return;
            }
            // A channel lost with its connection can report its close first; the reconnection
            // restores the listener, and rebuilding again would replace a working consumer.
            if (rebuildFor != channel) return;
        }
        if (!Connection.IsReady) return;
        lock (sync) rebuildFailures++;
        try
        {
            await RestoreAsync().ConfigureAwait(false);
            lock (sync) rebuildFailures = 0;
        }
        catch (Exception e)
        {
            Logger.Error($"{ListenerName}: failed to rebuild its channel: {e.Message}");
            ScheduleRebuild(e.Message);
        }
    }

    /// <summary>basic.cancel leaves the channel open, so neither recovery would notice it.</summary>
    private void OnConsumerCancelled(string tag)
    {
        lock (sync)
        {
            if (closing || !initialized || !wasStarted || consumerTag != tag) return;
            consumerTag = "";
            consumerLost = true;
        }
        ScheduleRebuild("the broker cancelled its consumer");
    }

    private async Task StartConsumingAsync()
    {
        var tag = Guid.NewGuid().ToString();
        IAmqpChannel? ch;
        string queue;
        MessageHandler h;
        bool anonymous;
        lock (sync)
        {
            consumerTag = tag;
            ch = channel;
            queue = queueName;
            h = handler!;
            anonymous = isAnonymous;
        }
        if (ch == null) throw new NotConnectedError($"{ListenerName}: no channel to consume on");
        var options = new ConsumeOptions
        {
            ConsumerTag = tag,
            Exclusive = anonymous,
            Ordered = OrderedDelivery,
            BuildErrorReply = BuildErrorReply,
            OnCancelled = () => OnConsumerCancelled(tag),
            // An early-ack consumer gets no prefetch from the broker, so its parallelism is
            // bounded here.
            MaxConcurrency = LateAck || OrderedDelivery ? 0 : Math.Max(1, MaxConcurrent ?? 1),
        };
        await Connection.ConsumeAsync(ch, queue, h, options, LateAck, GetRetryOptions(), ProcessingTimeoutMs).ConfigureAwait(false);
        Logger.Debug($"{ListenerName}: started consuming from {queue}");
    }

    /// <summary>Retry options for consume; subclasses override to enable retry.</summary>
    protected virtual ConsumeRetryOptions? GetRetryOptions() => null;

    /// <summary>
    /// Open the channel and declare the exchange and the queue: <paramref name="queueName"/>, or an
    /// exclusive, server-named one when it is null or empty.
    /// </summary>
    public virtual async Task InitAsync(MessageHandler? messageHandler, string? queueName)
    {
        lock (sync)
            if (initialized) return;
        if (ExchangeName.Length == 0) throw new ProtobusException("a listener needs an exchange");
        if (!Connection.IsConnected) throw new NotConnectedError();
        var anonymous = string.IsNullOrEmpty(queueName);
        lock (sync)
        {
            handler = messageHandler ?? DefaultHandler;
            isAnonymous = anonymous;
            configuredQueueName = anonymous ? "" : queueName!;
            closing = false;
        }
        AttachRestorer();
        var ch = await Connection.OpenChannelAsync().ConfigureAwait(false);
        if (LateAck) await ch.PrefetchAsync(EffectivePrefetch()).ConfigureAwait(false);
        await Connection.DeclareExchangeAsync(ch, ExchangeName, ExchangeType).ConfigureAwait(false);
        var name = await Connection.DeclareQueueAsync(ch, anonymous ? "" : queueName!, !anonymous, anonymous, anonymous,
            BuildQueueArguments()).ConfigureAwait(false);
        // A direct-exchange listener is bound to its own name.
        if (ExchangeType == "direct") await ch.BindQueueAsync(name, ExchangeName, name).ConfigureAwait(false);
        lock (sync)
        {
            channel = ch;
            this.queueName = name;
            initialized = true;
        }
        WatchChannel(ch);
    }

    public async Task StartAsync()
    {
        lock (sync)
        {
            if (!initialized) throw new NotInitializedError();
            if (wasStarted && (consumerTag.Length > 0 || consumerLost)) throw new AlreadyStartedError();
        }
        if (!Connection.IsConnected) throw new NotConnectedError();
        AttachRestorer();
        await restoreLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await StartConsumingAsync().ConfigureAwait(false);
            lock (sync) wasStarted = true;
        }
        finally
        {
            restoreLock.Release();
        }
    }

    /// <summary>
    /// Stop accepting NEW deliveries, leaving the channel open so work in hand can still ack and
    /// reply: the first step of a graceful shutdown. Serialised with restoration.
    /// </summary>
    public async Task StopConsumingAsync()
    {
        await restoreLock.WaitAsync().ConfigureAwait(false);
        try
        {
            string tag;
            IAmqpChannel? ch;
            lock (sync)
            {
                tag = consumerTag;
                consumerTag = "";
                wasStarted = false;
                ch = channel;
            }
            DetachRestorer();
            if (tag.Length == 0 || ch == null || !Connection.IsConnected) return;
            try
            {
                await ch.CancelAsync(tag).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug($"{ListenerName}: failed to cancel consumer '{tag}' during drain: {e.Message}");
            }
        }
        finally
        {
            restoreLock.Release();
        }
    }

    public async Task CloseAsync()
    {
        string tag;
        IAmqpChannel? ch;
        lock (sync)
        {
            if (!initialized) return;
            closing = true;
            tag = consumerTag;
            ch = channel;
        }
        DetachRestorer();
        Connection.Disconnected -= OnDisconnected;
        if (ch != null && Connection.IsConnected)
        {
            try
            {
                if (tag.Length > 0) await ch.CancelAsync(tag).ConfigureAwait(false);
                await ch.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug($"{ListenerName}: error during close (may be expected): {e.Message}");
            }
        }
        lock (sync)
        {
            consumerTag = "";
            channel = null;
            initialized = false;
            wasStarted = false;
            bindings.Clear();
        }
    }

    /// <summary>
    /// The main queue's arguments. Each key is added only when its option is set, so a listener
    /// that configures nothing declares an empty table, as every port does.
    /// </summary>
    protected virtual Dictionary<string, object?> BuildQueueArguments()
    {
        var args = new Dictionary<string, object?>();
        if (MessageTtlMs != null) args["x-message-ttl"] = MessageTtlMs.Value;
        if (MaxPriority != null) args["x-max-priority"] = MaxPriority.Value;
        return args;
    }

    /// <summary>
    /// Prefetch for late-ack consumers. 0 would mean unlimited, which with late ack lets the broker
    /// push a whole backlog into memory.
    /// </summary>
    protected ushort EffectivePrefetch() =>
        (ushort)(MaxConcurrent is > 0 ? Math.Min(MaxConcurrent.Value, 65535) : Math.Min(Config.DefaultPrefetch, 65535));

    /// <summary>Bind the queue under a routing key, and remember it for restoration.</summary>
    protected async Task BindAsync(string routingKey)
    {
        var ch = Channel ?? throw new NotConnectedError($"{ListenerName}: not connected");
        await ch.BindQueueAsync(QueueName, ExchangeName, routingKey).ConfigureAwait(false);
        lock (sync)
            if (!bindings.Contains(routingKey)) bindings.Add(routingKey);
    }
}

/// <summary>The context's reply queue: exclusive, server-named, handled in delivery order.</summary>
public sealed class CallbackListener : BaseListener
{
    public CallbackListener(Connection connection) : base(connection)
    {
        ExchangeName = Config.CallbacksExchangeName;
        ExchangeType = "direct";
        OrderedDelivery = true;
    }
}

/// <summary>
/// Hears stream-cancellation notices and stops the matching in-flight stream in this process.
/// Best effort by design: a deployment that cannot declare the cancel exchange keeps working.
/// </summary>
public sealed class CancelListener
{
    private readonly Connection connection;
    private readonly object sync = new();
    private IAmqpChannel? channel;
    private string consumerTag = "";
    private bool started;
    private readonly IDisposable detachRestorer;

    public CancelListener(Connection connection)
    {
        this.connection = connection;
        detachRestorer = connection.RegisterRestorer(async _ =>
        {
            lock (sync)
                if (!started) return;
            await StartAsync().ConfigureAwait(false);
        });
    }

    public async Task StartAsync()
    {
        try
        {
            var ch = await connection.OpenChannelAsync().ConfigureAwait(false);
            await connection.DeclareExchangeAsync(ch, Config.CancelExchangeName, "fanout").ConfigureAwait(false);
            var queue = await connection.DeclareQueueAsync(ch, "", false, true, true).ConfigureAwait(false);
            await ch.BindQueueAsync(queue, Config.CancelExchangeName, "").ConfigureAwait(false);
            var tag = await ch.ConsumeAsync(queue, "", true, true, delivery =>
            {
                var id = delivery.Properties.CorrelationId;
                // Every replica hears every cancel; only the one running that stream acts.
                if (!string.IsNullOrEmpty(id)) connection.CancelStream(id);
                return Task.CompletedTask;
            }, null).ConfigureAwait(false);
            lock (sync)
            {
                channel = ch;
                consumerTag = tag;
                started = true;
            }
        }
        catch (Exception e)
        {
            Logger.Warn($"CancelListener: stream cancellation unavailable ({e.Message}). Streams will run to completion; "
                + "everything else is unaffected.");
            lock (sync)
            {
                started = true;
                channel = null;
            }
        }
    }

    public async Task CloseAsync()
    {
        detachRestorer.Dispose();
        IAmqpChannel? ch;
        string tag;
        lock (sync)
        {
            ch = channel;
            tag = consumerTag;
            channel = null;
            consumerTag = "";
            started = false;
        }
        if (ch != null && connection.IsConnected)
        {
            try
            {
                if (tag.Length > 0) await ch.CancelAsync(tag).ConfigureAwait(false);
                await ch.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug("CancelListener: error during close: " + e.Message);
            }
        }
    }
}

/// <summary>A service's request queue, with its retry queue, retry exchange and DLQ.</summary>
public sealed class MessageListener : BaseListener
{
    private readonly RetryOptions retryConfig;
    private volatile string dlqName = "";
    private volatile string retryQueueName = "";
    private volatile string retryExchangeName = "";

    public MessageListener(Connection connection, bool lateAck, int? maxConcurrent, RetryOptions? retry,
        long? processingTimeoutMs, int? maxPriority) : base(connection)
    {
        MaxPriority = Priority.ValidateMaxPriority(maxPriority);
        // Priority can only reorder messages still in the queue, and an early-ack consumer has no
        // prefetch: the broker hands it the whole backlog. Refused, because it would do nothing.
        if (MaxPriority != null && !lateAck)
            throw new InvalidPriorityError("maxPriority requires lateAck: with lateAck off the consumer acks on delivery, "
                + "RabbitMQ applies no prefetch, and priority has nothing left to reorder.");
        ExchangeName = Config.BusExchangeName;
        ExchangeType = "topic";
        LateAck = lateAck;
        MaxConcurrent = maxConcurrent is > 0 ? maxConcurrent : 1;
        ProcessingTimeoutMs = processingTimeoutMs;
        retryConfig = retry ?? new RetryOptions();
        MessageTtlMs = retryConfig.MessageTtlMs;
    }

    /// <summary>Declare the DLQ, the retry queue and the retry exchange, after the main queue exists.</summary>
    private async Task SetupRetryQueuesAsync()
    {
        if (retryConfig.MaxRetries <= 0 || IsAnonymous) return;
        var ch = Channel!;
        var service = ConfiguredQueueName;
        var dlq = service + ".DLQ";
        var retryQueue = service + ".Retry";
        var retryExchange = service + ".Retry.Exchange";
        await Connection.DeclareQueueAsync(ch, dlq, true, false, false).ConfigureAwait(false);
        try
        {
            await Connection.DeclareQueueAsync(ch, retryQueue, true, false, false, new Dictionary<string, object?>
            {
                ["x-message-ttl"] = retryConfig.RetryDelayMs,
                // No x-dead-letter-routing-key: the message's own routing key is kept.
                ["x-dead-letter-exchange"] = ExchangeName,
            }).ConfigureAwait(false);
        }
        catch (AmqpException e) when (e.PreconditionFailed)
        {
            throw new RetryQueueMismatchError($"retry queue '{retryQueue}' already exists with different arguments "
                + $"(most likely a different retryDelayMs, now {retryConfig.RetryDelayMs}ms). RabbitMQ cannot change a "
                + "queue's x-message-ttl in place: drain and delete the queue, or keep the original retryDelayMs. "
                + $"Original error: {e.Message}");
        }
        await Connection.DeclareExchangeAsync(ch, retryExchange, "topic").ConfigureAwait(false);
        await ch.BindQueueAsync(retryQueue, retryExchange, "#").ConfigureAwait(false);
        dlqName = dlq;
        retryQueueName = retryQueue;
        retryExchangeName = retryExchange;
    }

    protected override Task RestoreTopologyAsync() =>
        retryQueueName.Length > 0 ? SetupRetryQueuesAsync() : Task.CompletedTask;

    protected override ConsumeRetryOptions? GetRetryOptions() =>
        retryConfig.MaxRetries <= 0 || retryQueueName.Length == 0 || dlqName.Length == 0
            ? null
            : new ConsumeRetryOptions(retryConfig.MaxRetries, retryQueueName, retryExchangeName, dlqName, Errors.IsHandledError);

    /// <summary>Bind the queue to the topics, then declare the retry topology.</summary>
    public async Task SubscribeAsync(params string[] topics)
    {
        foreach (var t in topics) await BindAsync(t).ConfigureAwait(false);
        await SetupRetryQueuesAsync().ConfigureAwait(false);
    }

    internal void SetBuildErrorReply(Func<byte[], Exception, byte[]?> build) => BuildErrorReply = build;
}

/// <summary>Handles one event. Throwing fails the delivery: dropped, or retried when event retry is on.</summary>
public delegate Task EventCallback<in T>(T @event, string type, string topic) where T : IMessage;

/// <summary>
/// A service's event queue and the handlers subscribed on it, matched by topic pattern as the
/// broker matches bindings.
/// </summary>
public sealed class EventListener : BaseListener
{
    private readonly EventRetryOptions retryConfig;
    private readonly Trie<Subscription> router = new();
    private readonly object routerLock = new();
    private volatile string retryQueueName = "";
    private volatile string retryExchangeName = "";
    private volatile string dlqName = "";

    private sealed class Subscription
    {
        internal readonly MessageParser Parser;
        internal readonly string TypeName;
        internal readonly Func<IMessage, string, string, Task> Handler;

        internal Subscription(MessageParser parser, string typeName, Func<IMessage, string, string, Task> handler)
        {
            Parser = parser;
            TypeName = typeName;
            Handler = handler;
        }
    }

    public EventListener(Connection connection, EventRetryOptions? retry = null) : base(connection)
    {
        retryConfig = retry ?? new EventRetryOptions();
        ExchangeName = Config.EventsExchangeName;
        ExchangeType = "topic";
        LateAck = true;
    }

    protected override async Task<HandlerResult> DefaultHandler(byte[] encodedEvent, string correlationId,
        MessageHandlerContext context)
    {
        Envelopes.Event e;
        try
        {
            e = Envelopes.DecodeEvent(encodedEvent);
        }
        catch (Envelopes.MalformedException)
        {
            throw new ProtocolError("event envelope did not decode");
        }
        // The routing key the broker delivered on, not the topic in the body: the body is
        // publisher-controlled, and trusting it would let a publisher reach handlers its routing
        // key was never permitted to reach.
        var matchTopic = string.IsNullOrEmpty(context.RoutingKey) ? e.Topic : context.RoutingKey;
        if (string.IsNullOrEmpty(matchTopic))
        {
            Logger.Warn($"ignoring unhandled event of type '{e.Type}' (no topic to route on)");
            return HandlerResult.None;
        }
        List<Subscription> handlers;
        lock (routerLock) handlers = router.Match(matchTopic);
        foreach (var s in handlers)
        {
            if (s.TypeName != e.Type)
            {
                // The topic matched, but the payload is another type: decoding it as this
                // handler's type would hand it garbage.
                Logger.Warn($"event of type '{e.Type}' on {e.Topic} skipped by a handler of {s.TypeName}");
                continue;
            }
            IMessage decoded;
            try
            {
                decoded = s.Parser.ParseFrom(e.Data);
            }
            catch (InvalidProtocolBufferException)
            {
                throw new ProtocolError($"event of type {e.Type} did not decode");
            }
            CustomTypes.Validate(decoded);
            await s.Handler(decoded, e.Type, e.Topic).ConfigureAwait(false);
        }
        return HandlerResult.None;
    }

    /// <summary>
    /// Declare the retry ladder. An event's retry queue cannot dead-letter back to the events
    /// exchange (every subscriber would get it again): it goes to a per-subscriber exchange bound
    /// only to this listener's queue.
    /// </summary>
    private async Task SetupRetryTopologyAsync()
    {
        if (retryConfig.MaxRetries <= 0 || IsAnonymous) return;
        var ch = Channel!;
        var @base = ConfiguredQueueName;
        var dlq = @base + ".DLQ";
        var retryQueue = @base + ".Retry";
        var retryExchange = @base + ".Retry.Exchange";
        var redelivery = @base + ".Redelivery";
        await Connection.DeclareQueueAsync(ch, dlq, true, false, false).ConfigureAwait(false);
        await Connection.DeclareExchangeAsync(ch, redelivery, "topic").ConfigureAwait(false);
        await ch.BindQueueAsync(QueueName, redelivery, "#").ConfigureAwait(false);
        try
        {
            await Connection.DeclareQueueAsync(ch, retryQueue, true, false, false, new Dictionary<string, object?>
            {
                ["x-message-ttl"] = retryConfig.RetryDelayMs,
                ["x-dead-letter-exchange"] = redelivery,
            }).ConfigureAwait(false);
        }
        catch (AmqpException e) when (e.PreconditionFailed)
        {
            throw new RetryQueueMismatchError($"event retry queue '{retryQueue}' already exists with different arguments "
                + $"(most likely a different retryDelayMs, now {retryConfig.RetryDelayMs}ms). Original error: {e.Message}");
        }
        await Connection.DeclareExchangeAsync(ch, retryExchange, "topic").ConfigureAwait(false);
        await ch.BindQueueAsync(retryQueue, retryExchange, "#").ConfigureAwait(false);
        dlqName = dlq;
        retryQueueName = retryQueue;
        retryExchangeName = retryExchange;
    }

    protected override Task RestoreTopologyAsync() =>
        retryQueueName.Length > 0 ? SetupRetryTopologyAsync() : Task.CompletedTask;

    protected override ConsumeRetryOptions? GetRetryOptions() =>
        retryConfig.MaxRetries <= 0 || retryQueueName.Length == 0 || dlqName.Length == 0
            ? null
            : new ConsumeRetryOptions(retryConfig.MaxRetries, retryQueueName, retryExchangeName, dlqName, Errors.IsHandledError);

    public override async Task InitAsync(MessageHandler? messageHandler, string? queueName)
    {
        if (IsInitialized) return;
        await base.InitAsync(messageHandler, queueName).ConfigureAwait(false);
        // Before start, so the first delivery already has somewhere to fail to.
        await SetupRetryTopologyAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Subscribe a typed handler. The topic defaults to <c>EVENT.&lt;type&gt;</c>; a pattern
    /// receives every matching event of this handler's type.
    /// </summary>
    public Task SubscribeAsync<T>(EventCallback<T> handler, string? topic = null) where T : IMessage<T>, new()
    {
        var descriptor = new T().Descriptor;
        var type = descriptor.FullName;
        var parser = (MessageParser)descriptor.Parser;
        var s = new Subscription(parser, type, (m, t, p) => handler((T)m, t, p));
        lock (routerLock) router.Add(string.IsNullOrEmpty(topic) ? "EVENT." + type : topic, s);
        return BindAsync(string.IsNullOrEmpty(topic) ? "EVENT." + type : topic);
    }

    /// <summary>The retry objects' names, or null when event retry is off.</summary>
    public (string RetryQueue, string Dlq)? RetryTopology => retryQueueName.Length == 0 ? null : (retryQueueName, dlqName);
}
