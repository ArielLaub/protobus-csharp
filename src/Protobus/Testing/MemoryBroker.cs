using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Protobus.Amqp;
using Lanes = System.Threading.Channels;

namespace Protobus.Testing;

/// <summary>
/// An in-memory AMQP broker, for testing services and clients without RabbitMQ.
/// <code>
/// var broker = new MemoryBroker();
/// await using var ctx = new Context(new ContextOptions { Transport = broker });
/// await ctx.InitAsync("amqp://memory/");
/// </code>
/// It implements the RabbitMQ behaviour protobus depends on: direct, topic and fanout exchanges
/// and the default exchange; durable, exclusive, auto-delete and server-named queues; per-queue
/// message TTL with dead-lettering; priority queues; per-consumer prefetch with acknowledgement,
/// rejection and redelivery; publisher confirms with mandatory returns; and channel-closing errors
/// for a missing exchange or queue (404), a redeclaration with different arguments (406), and an
/// exclusive queue owned elsewhere (405).
///
/// Deliveries run one at a time per channel, each awaited before the next, as RabbitMQ.Client
/// dispatches them; confirms, cancels and closes run in order on one event lane of the broker's
/// own.
///
/// Fault injection: <see cref="KillConnections"/> drops every connection as a network failure
/// would, <see cref="RefuseConnections"/> makes connect fail, and <see cref="SetConfirmMode"/>
/// makes the broker nack or never confirm publishes.
/// </summary>
public sealed class MemoryBroker : ITransport, IAsyncDisposable
{
    public enum ConfirmMode
    {
        /// <summary>Confirm every publish (the default).</summary>
        Ack,
        /// <summary>Refuse every publish.</summary>
        Nack,
        /// <summary>Never confirm: publishes wait out their confirm timeout.</summary>
        Drop,
    }

    private readonly object sync = new();
    private readonly Lane events = new();
    private readonly CancellationTokenSource stopping = new();

    private readonly Dictionary<string, Exchange> exchanges = new();
    private readonly Dictionary<string, Queue> queues = new();
    private readonly List<Conn> connections = new();
    private bool refuse;
    private ConfirmMode confirmMode = ConfirmMode.Ack;
    private int connectAttempts;
    private long nextId;

    public MemoryBroker()
    {
        // RabbitMQ's predeclared default exchange.
        exchanges[""] = new Exchange("", "direct", true, false, new Dictionary<string, object?>());
    }

    // ---- model -------------------------------------------------------------------------

    /// <summary>A serial work queue: each item is awaited before the next starts.</summary>
    private sealed class Lane
    {
        private readonly Lanes.Channel<Func<Task>> work = Lanes.Channel.CreateUnbounded<Func<Task>>(
            new Lanes.UnboundedChannelOptions { SingleReader = true });

        internal Lane() => _ = Task.Run(RunAsync);

        private async Task RunAsync()
        {
            await foreach (var item in work.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    await item().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("memory-broker: callback failed: " + e);
                }
            }
        }

        internal void Post(Func<Task> item) => work.Writer.TryWrite(item);

        internal Task FlushAsync()
        {
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!work.Writer.TryWrite(() =>
                {
                    done.TrySetResult();
                    return Task.CompletedTask;
                }))
                done.TrySetResult();
            return done.Task;
        }

        internal void Complete() => work.Writer.TryComplete();
    }

    /// <summary>Callbacks collected under the lock, posted once it is released.</summary>
    private sealed class Callbacks : List<(Lane Lane, Func<Task> Work)>
    {
        internal void Event(Lane lane, Action a) => Add((lane, () =>
        {
            a();
            return Task.CompletedTask;
        }));
    }

    private sealed class Exchange
    {
        internal readonly string Name;
        internal readonly string Type;
        internal readonly bool Durable;
        internal readonly bool AutoDelete;
        internal readonly IDictionary<string, object?> Arguments;
        internal readonly List<(string Queue, string Key)> Bindings = new();

        internal Exchange(string name, string type, bool durable, bool autoDelete, IDictionary<string, object?> arguments)
        {
            Name = name;
            Type = type;
            Durable = durable;
            AutoDelete = autoDelete;
            Arguments = arguments;
        }
    }

    private sealed class Message
    {
        internal readonly byte[] Body;
        internal readonly MessageProperties Properties;
        internal readonly string Exchange;
        internal readonly string RoutingKey;
        internal bool Redelivered;

        internal Message(byte[] body, MessageProperties properties, string exchange, string routingKey)
        {
            Body = body;
            Properties = properties;
            Exchange = exchange;
            RoutingKey = routingKey;
        }
    }

    private sealed class Queue
    {
        internal readonly string Name;
        internal readonly bool Durable;
        internal readonly bool Exclusive;
        internal readonly bool AutoDelete;
        internal readonly IDictionary<string, object?> Arguments;
        internal readonly Conn? Owner;
        internal readonly List<Message> Messages = new();
        internal readonly List<ConsumerEntry> Consumers = new();
        internal int RoundRobin;
        internal bool HadConsumer;
        internal int Unacked;

        internal Queue(string name, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?> arguments,
            Conn? owner)
        {
            Name = name;
            Durable = durable;
            Exclusive = exclusive;
            AutoDelete = autoDelete;
            Arguments = arguments;
            Owner = owner;
        }

        internal long? Ttl => Arguments.TryGetValue("x-message-ttl", out var v) && IsInteger(v) ? Convert.ToInt64(v) : null;

        private int? MaxPriority =>
            Arguments.TryGetValue("x-max-priority", out var v) && IsInteger(v) ? Convert.ToInt32(v) : null;

        private int PriorityOf(Message m) => MaxPriority is { } max ? Math.Min(m.Properties.Priority ?? 0, max) : 0;

        /// <summary>Insert keeping higher priorities first and FIFO within one. Requeued messages go first in their level.</summary>
        internal void Enqueue(Message m, bool atHead)
        {
            var p = PriorityOf(m);
            var i = 0;
            if (atHead)
                while (i < Messages.Count && PriorityOf(Messages[i]) > p) i++;
            else
                while (i < Messages.Count && PriorityOf(Messages[i]) >= p) i++;
            Messages.Insert(i, m);
        }
    }

    private sealed class ConsumerEntry
    {
        internal readonly string Tag;
        internal readonly Chan Channel;
        internal readonly Queue Queue;
        internal readonly bool NoAck;
        internal readonly int Prefetch;
        internal readonly Func<Delivery, Task> OnDelivery;
        internal readonly Action? OnCancel;
        internal int Unacked;

        internal ConsumerEntry(string tag, Chan channel, Queue queue, bool noAck, int prefetch,
            Func<Delivery, Task> onDelivery, Action? onCancel)
        {
            Tag = tag;
            Channel = channel;
            Queue = queue;
            NoAck = noAck;
            Prefetch = prefetch;
            OnDelivery = onDelivery;
            OnCancel = onCancel;
        }
    }

    private sealed record Unacked(Queue Queue, Message Message, ConsumerEntry Consumer);

    private static bool IsInteger(object? v) => v is sbyte or byte or short or ushort or int or uint or long or ulong;

    // ---- transport ---------------------------------------------------------------------

    public Task<IAmqpConnection> ConnectAsync(string url, int heartbeatSeconds)
    {
        lock (sync)
        {
            connectAttempts++;
            if (refuse)
                return Task.FromException<IAmqpConnection>(new AmqpException("connect failed: connection refused (memory broker)"));
            var c = new Conn(this);
            connections.Add(c);
            return Task.FromResult<IAmqpConnection>(c);
        }
    }

    private sealed class Conn : IAmqpConnection
    {
        private readonly MemoryBroker broker;
        internal bool Open = true;
        internal readonly List<Chan> Channels = new();
        internal readonly List<Action<string?>> CloseListeners = new();
        internal string? CloseReason;
        internal bool ClosedGracefully;

        internal Conn(MemoryBroker broker) => this.broker = broker;

        public Task<IAmqpChannel> OpenChannelAsync()
        {
            lock (broker.sync)
            {
                if (!Open)
                    return Task.FromException<IAmqpChannel>(new AmqpException("open channel failed: connection is closed", 320));
                var ch = new Chan(broker, this);
                Channels.Add(ch);
                return Task.FromResult<IAmqpChannel>(ch);
            }
        }

        public Task CloseAsync()
        {
            var cb = new Callbacks();
            lock (broker.sync)
            {
                if (!Open) return Task.CompletedTask;
                ClosedGracefully = true;
                broker.CloseConnection(this, null, cb);
            }
            broker.Post(cb);
            return Task.CompletedTask;
        }

        public bool IsOpen
        {
            get
            {
                lock (broker.sync) return Open;
            }
        }

        public void OnClose(Action<string?> listener)
        {
            bool now;
            string? reason;
            lock (broker.sync)
            {
                now = !Open;
                reason = ClosedGracefully ? null : CloseReason;
                if (!now) CloseListeners.Add(listener);
            }
            if (now) broker.events.Post(() =>
            {
                listener(reason);
                return Task.CompletedTask;
            });
        }
    }

    private sealed class Chan : IAmqpChannel
    {
        private readonly MemoryBroker broker;
        internal readonly Conn Conn;
        /// <summary>This channel's deliveries, one at a time.</summary>
        internal readonly Lane Deliveries = new();
        internal bool Open = true;
        private int prefetch;
        internal ulong NextDeliveryTag = 1;
        internal readonly Dictionary<ulong, Unacked> Unacked = new();
        internal readonly Dictionary<string, ConsumerEntry> Consumers = new();
        internal readonly List<Action<string>> CloseListeners = new();
        internal readonly Queue<Action<ConfirmOutcome, string>> Held = new();
        internal string CloseReason = "";

        internal Chan(MemoryBroker broker, Conn conn)
        {
            this.broker = broker;
            Conn = conn;
        }

        /// <summary>A channel exception: closes this channel, then is thrown.</summary>
        private AmqpException Fail(int code, string text, Callbacks cb)
        {
            broker.CloseChannel(this, code + " " + text, cb);
            return new AmqpException(text, code);
        }

        /// <summary>Run <paramref name="op"/> under the broker's lock on an open channel; a failure faults the task.</summary>
        private Task<T> Op<T>(string what, Func<Callbacks, T> op)
        {
            var cb = new Callbacks();
            try
            {
                lock (broker.sync)
                {
                    if (!Open) throw new AmqpException($"{what} failed: channel is closed ({CloseReason})", 504);
                    return Task.FromResult(op(cb));
                }
            }
            catch (Exception e)
            {
                return Task.FromException<T>(e);
            }
            finally
            {
                broker.Post(cb);
            }
        }

        private Task Op(string what, Action<Callbacks> op) => Op<bool>(what, cb =>
        {
            op(cb);
            return true;
        });

        public Task DeclareExchangeAsync(string name, string type, bool durable, bool autoDelete, bool @internal,
            IDictionary<string, object?> arguments) =>
            Op("exchange.declare", cb =>
            {
                if (broker.exchanges.TryGetValue(name, out var existing))
                {
                    if (existing.Type != type || existing.Durable != durable || existing.AutoDelete != autoDelete)
                        throw Fail(406, $"PRECONDITION_FAILED - inequivalent arg 'type' for exchange '{name}'", cb);
                    return;
                }
                broker.exchanges[name] = new Exchange(name, type, durable, autoDelete, Copy(arguments));
            });

        public Task<string> DeclareQueueAsync(string name, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object?> arguments) =>
            Op("queue.declare", cb =>
            {
                var actual = string.IsNullOrEmpty(name) ? "amq.gen-" + Guid.NewGuid() : name;
                if (broker.queues.TryGetValue(actual, out var existing))
                {
                    if (existing.Exclusive && existing.Owner != Conn)
                        throw Fail(405, $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{actual}'", cb);
                    if (existing.Durable != durable || existing.Exclusive != exclusive || existing.AutoDelete != autoDelete
                        || !SameArguments(existing.Arguments, arguments))
                        throw Fail(406, $"PRECONDITION_FAILED - inequivalent arg for queue '{actual}'", cb);
                    return actual;
                }
                broker.queues[actual] = new Queue(actual, durable, exclusive, autoDelete, Copy(arguments),
                    exclusive ? Conn : null);
                return actual;
            });

        public Task BindQueueAsync(string queue, string exchange, string routingKey) =>
            Op("queue.bind", cb =>
            {
                if (!broker.exchanges.TryGetValue(exchange, out var x))
                    throw Fail(404, $"NOT_FOUND - no exchange '{exchange}'", cb);
                if (!broker.queues.ContainsKey(queue)) throw Fail(404, $"NOT_FOUND - no queue '{queue}'", cb);
                if (exchange.Length == 0) throw Fail(403, "ACCESS_REFUSED - cannot bind to the default exchange", cb);
                if (!x.Bindings.Contains((queue, routingKey))) x.Bindings.Add((queue, routingKey));
            });

        public Task UnbindQueueAsync(string queue, string exchange, string routingKey) =>
            Op("queue.unbind", _ =>
            {
                if (broker.exchanges.TryGetValue(exchange, out var x)) x.Bindings.Remove((queue, routingKey));
            });

        public Task DeleteQueueAsync(string name) =>
            Op("queue.delete", cb =>
            {
                if (broker.queues.TryGetValue(name, out var q)) broker.DeleteQueueLocked(q, cb);
            });

        public Task PurgeQueueAsync(string name) =>
            Op("queue.purge", cb =>
            {
                if (!broker.queues.TryGetValue(name, out var q)) throw Fail(404, $"NOT_FOUND - no queue '{name}'", cb);
                q.Messages.Clear();
            });

        public Task PrefetchAsync(ushort count) => Op("basic.qos", _ => prefetch = count);

        public Task<string> ConsumeAsync(string queue, string consumerTag, bool noAck, bool exclusive,
            Func<Delivery, Task> onDelivery, Action? onCancel) =>
            Op("basic.consume", cb =>
            {
                if (!broker.queues.TryGetValue(queue, out var q)) throw Fail(404, $"NOT_FOUND - no queue '{queue}'", cb);
                if (q.Exclusive && q.Owner != Conn)
                    throw Fail(405, $"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '{queue}'", cb);
                if (exclusive && q.Consumers.Count > 0) throw Fail(403, $"ACCESS_REFUSED - queue '{queue}' in use", cb);
                var tag = string.IsNullOrEmpty(consumerTag) ? "amq.ctag-" + Guid.NewGuid() : consumerTag;
                if (Consumers.ContainsKey(tag)) throw Fail(530, "NOT_ALLOWED - attempt to reuse consumer tag", cb);
                var c = new ConsumerEntry(tag, this, q, noAck, prefetch, onDelivery, onCancel);
                Consumers[tag] = c;
                q.Consumers.Add(c);
                q.HadConsumer = true;
                broker.Pump(q, cb);
                return tag;
            });

        public Task CancelAsync(string consumerTag) =>
            Op("basic.cancel", cb =>
            {
                if (Consumers.Remove(consumerTag, out var c)) broker.RemoveConsumer(c, cb);
            });

        public Task AckAsync(ulong deliveryTag) =>
            Op("basic.ack", cb =>
            {
                if (!Unacked.Remove(deliveryTag, out var u))
                    throw Fail(406, $"PRECONDITION_FAILED - unknown delivery tag {deliveryTag}", cb);
                broker.Settle(u, cb);
            });

        public Task RejectAsync(ulong deliveryTag, bool requeue) =>
            Op("basic.reject", cb =>
            {
                if (!Unacked.Remove(deliveryTag, out var u))
                    throw Fail(406, $"PRECONDITION_FAILED - unknown delivery tag {deliveryTag}", cb);
                broker.Settle(u, cb);
                if (requeue && broker.queues.GetValueOrDefault(u.Queue.Name) == u.Queue)
                {
                    u.Message.Redelivered = true;
                    u.Queue.Enqueue(u.Message, true);
                    broker.Pump(u.Queue, cb);
                }
                else
                {
                    broker.DeadLetter(u.Queue, u.Message, "rejected", cb);
                }
            });

        public Task PublishAsync(string exchange, string routingKey, byte[] body, MessageProperties properties,
            bool mandatory, Action<ConfirmOutcome, string> onConfirm) =>
            Op("basic.publish", cb =>
            {
                if (!broker.exchanges.TryGetValue(exchange ?? "", out var x))
                {
                    // Asynchronous in AMQP: the channel closes, and the publish's confirm never
                    // comes: it is reported closed.
                    var reason = $"404 NOT_FOUND - no exchange '{exchange}'";
                    cb.Event(broker.events, () => onConfirm(ConfirmOutcome.Closed, reason));
                    broker.CloseChannel(this, reason, cb);
                    return;
                }
                var m = new Message((byte[])body.Clone(), properties, x.Name, routingKey);
                var routed = broker.Route(x, m, cb);
                ConfirmOutcome? outcome = broker.confirmMode switch
                {
                    ConfirmMode.Nack => ConfirmOutcome.Nack,
                    ConfirmMode.Drop => null,
                    _ => routed == 0 && mandatory ? ConfirmOutcome.Returned : ConfirmOutcome.Ack,
                };
                if (outcome is not { } o) Held.Enqueue(onConfirm);
                else cb.Event(broker.events, () => onConfirm(o, o == ConfirmOutcome.Nack ? "basic.nack" : ""));
            });

        public Task CloseAsync()
        {
            var cb = new Callbacks();
            lock (broker.sync)
            {
                if (!Open) return Task.CompletedTask;
                broker.CloseChannel(this, "200 OK (closed by the client)", cb);
            }
            broker.Post(cb);
            return Task.CompletedTask;
        }

        public bool IsOpen
        {
            get
            {
                lock (broker.sync) return Open;
            }
        }

        public void OnClose(Action<string> listener)
        {
            bool now;
            string reason;
            lock (broker.sync)
            {
                now = !Open;
                reason = CloseReason;
                if (!now) CloseListeners.Add(listener);
            }
            if (now) broker.events.Post(() =>
            {
                listener(reason);
                return Task.CompletedTask;
            });
        }
    }

    // ---- broker internals (all under the lock) -----------------------------------------

    private static IDictionary<string, object?> Copy(IDictionary<string, object?>? m) =>
        m == null ? new Dictionary<string, object?>() : new Dictionary<string, object?>(m);

    /// <summary>RabbitMQ compares integer arguments by value, whatever their width.</summary>
    private static bool SameArguments(IDictionary<string, object?>? a, IDictionary<string, object?>? b)
    {
        var x = a ?? new Dictionary<string, object?>();
        var y = b ?? new Dictionary<string, object?>();
        if (x.Count != y.Count || x.Keys.Any(k => !y.ContainsKey(k))) return false;
        foreach (var (k, u) in x)
        {
            var v = y[k];
            if (IsInteger(u) && IsInteger(v))
            {
                if (Convert.ToInt64(u) != Convert.ToInt64(v)) return false;
            }
            else if (!Equals(u?.ToString(), v?.ToString()))
            {
                return false;
            }
        }
        return true;
    }

    internal static bool TopicMatches(string pattern, string key) => Match(pattern.Split('.'), 0, key.Split('.'), 0);

    private static bool Match(string[] p, int i, string[] k, int j)
    {
        if (i == p.Length) return j == k.Length;
        if (p[i] == "#")
        {
            for (var n = j; n <= k.Length; n++)
                if (Match(p, i + 1, k, n)) return true;
            return false;
        }
        if (j == k.Length) return false;
        return (p[i] == "*" || p[i] == k[j]) && Match(p, i + 1, k, j + 1);
    }

    /// <summary>Route to every matching queue; returns how many received it.</summary>
    private int Route(Exchange x, Message m, Callbacks cb)
    {
        var targets = new List<Queue>();
        if (x.Name.Length == 0)
        {
            if (queues.TryGetValue(m.RoutingKey, out var q)) targets.Add(q);
        }
        else
        {
            foreach (var (queue, key) in x.Bindings)
            {
                var hit = x.Type switch
                {
                    "fanout" => true,
                    "topic" => TopicMatches(key, m.RoutingKey),
                    _ => key == m.RoutingKey,
                };
                if (hit && queues.TryGetValue(queue, out var q) && !targets.Contains(q)) targets.Add(q);
            }
        }
        foreach (var q in targets)
        {
            var copy = new Message(m.Body, m.Properties, m.Exchange, m.RoutingKey);
            nextId++;
            q.Enqueue(copy, false);
            ScheduleExpiry(q, copy);
            Pump(q, cb);
        }
        return targets.Count;
    }

    private void ScheduleExpiry(Queue q, Message m)
    {
        if (q.Ttl is not { } ttl) return;
        _ = ExpireAsync(q, m, ttl);
    }

    private async Task ExpireAsync(Queue q, Message m, long ttl)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(ttl), stopping.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        var cb = new Callbacks();
        lock (sync)
        {
            if (queues.GetValueOrDefault(q.Name) != q || !q.Messages.Remove(m)) return;
            DeadLetter(q, m, "expired", cb);
        }
        Post(cb);
    }

    private void DeadLetter(Queue q, Message m, string reason, Callbacks cb)
    {
        if (!q.Arguments.TryGetValue("x-dead-letter-exchange", out var dlx) || dlx == null) return;
        if (!exchanges.TryGetValue(dlx.ToString()!, out var x)) return;
        var key = q.Arguments.TryGetValue("x-dead-letter-routing-key", out var dlrk) && dlrk != null
            ? dlrk.ToString()!
            : m.RoutingKey;
        var headers = m.Properties.Headers == null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(m.Properties.Headers);
        headers["x-first-death-reason"] = reason;
        headers["x-first-death-queue"] = q.Name;
        Route(x, new Message(m.Body, m.Properties with { Headers = headers, Expiration = null }, x.Name, key), cb);
    }

    private void Pump(Queue q, Callbacks cb)
    {
        while (q.Messages.Count > 0 && q.Consumers.Count > 0)
        {
            ConsumerEntry? target = null;
            var n = q.Consumers.Count;
            for (var i = 0; i < n; i++)
            {
                var c = q.Consumers[(q.RoundRobin + i) % n];
                if (c.NoAck || c.Prefetch == 0 || c.Unacked < c.Prefetch)
                {
                    target = c;
                    q.RoundRobin = (q.RoundRobin + i + 1) % n;
                    break;
                }
            }
            if (target == null) return;
            var m = q.Messages[0];
            q.Messages.RemoveAt(0);
            var ch = target.Channel;
            var tag = ch.NextDeliveryTag++;
            if (!target.NoAck)
            {
                ch.Unacked[tag] = new Unacked(q, m, target);
                target.Unacked++;
                q.Unacked++;
            }
            var d = new Delivery((byte[])m.Body.Clone(), m.Properties, m.Exchange, m.RoutingKey, target.Tag, tag, m.Redelivered);
            var consumer = target;
            cb.Add((ch.Deliveries, () =>
            {
                lock (sync)
                {
                    // A consumer cancelled, or a channel closed, after the delivery was queued:
                    // real brokers can still deliver it, but nobody would ack it here. Its requeue
                    // already happened on close.
                    if (!ch.Open) return Task.CompletedTask;
                }
                return consumer.OnDelivery(d);
            }));
        }
    }

    private void Settle(Unacked u, Callbacks cb)
    {
        u.Consumer.Unacked--;
        u.Queue.Unacked--;
        Pump(u.Queue, cb);
    }

    private void RemoveConsumer(ConsumerEntry c, Callbacks cb)
    {
        c.Queue.Consumers.Remove(c);
        if (c.Queue.AutoDelete && c.Queue.HadConsumer && c.Queue.Consumers.Count == 0) DeleteQueueLocked(c.Queue, cb);
    }

    private void DeleteQueueLocked(Queue q, Callbacks cb)
    {
        if (queues.GetValueOrDefault(q.Name) != q) return;
        queues.Remove(q.Name);
        foreach (var x in exchanges.Values) x.Bindings.RemoveAll(b => b.Queue == q.Name);
        foreach (var c in q.Consumers.ToList())
        {
            c.Channel.Consumers.Remove(c.Tag);
            q.Consumers.Remove(c);
            if (c.OnCancel is { } onCancel) cb.Event(events, onCancel);
        }
    }

    private void CloseChannel(Chan ch, string reason, Callbacks cb)
    {
        if (!ch.Open) return;
        ch.Open = false;
        ch.CloseReason = reason;
        ch.Conn.Channels.Remove(ch);
        // Unacknowledged deliveries go back to their queues, marked redelivered.
        var back = ch.Unacked.Values.ToList();
        ch.Unacked.Clear();
        foreach (var c in ch.Consumers.Values.ToList()) RemoveConsumer(c, cb);
        ch.Consumers.Clear();
        foreach (var u in back)
        {
            u.Consumer.Unacked--;
            u.Queue.Unacked--;
            if (queues.GetValueOrDefault(u.Queue.Name) == u.Queue)
            {
                u.Message.Redelivered = true;
                u.Queue.Enqueue(u.Message, true);
            }
        }
        foreach (var u in back)
            if (queues.GetValueOrDefault(u.Queue.Name) == u.Queue) Pump(u.Queue, cb);
        while (ch.Held.TryDequeue(out var held)) cb.Event(events, () => held(ConfirmOutcome.Closed, reason));
        foreach (var l in ch.CloseListeners) cb.Event(events, () => l(reason));
        ch.CloseListeners.Clear();
        var lane = ch.Deliveries;
        cb.Event(events, lane.Complete);
    }

    private void CloseConnection(Conn c, string? reason, Callbacks cb)
    {
        c.Open = false;
        c.CloseReason = reason;
        connections.Remove(c);
        foreach (var ch in c.Channels.ToList()) CloseChannel(ch, reason ?? "320 CONNECTION_FORCED", cb);
        // Exclusive queues die with the connection that owns them.
        foreach (var q in queues.Values.ToList())
            if (q.Exclusive && q.Owner == c) DeleteQueueLocked(q, cb);
        var reported = c.ClosedGracefully ? null : reason;
        foreach (var l in c.CloseListeners) cb.Event(events, () => l(reported));
        c.CloseListeners.Clear();
    }

    private void Post(Callbacks cb)
    {
        foreach (var (lane, work) in cb) lane.Post(work);
    }

    // ---- fault injection ---------------------------------------------------------------

    /// <summary>
    /// Drop every open connection as a lost socket would: unacknowledged deliveries are requeued,
    /// exclusive queues deleted, and each connection's close listeners receive <paramref name="reason"/>.
    /// </summary>
    public void KillConnections(string reason = "connection reset by peer")
    {
        var cb = new Callbacks();
        lock (sync)
            foreach (var c in connections.ToList()) CloseConnection(c, reason, cb);
        Post(cb);
    }

    /// <summary>While set, connect fails.</summary>
    public void RefuseConnections(bool value)
    {
        lock (sync) refuse = value;
    }

    public void SetConfirmMode(ConfirmMode mode)
    {
        lock (sync) confirmMode = mode;
    }

    /// <summary>The publishes <see cref="ConfirmMode.Drop"/> is holding, across every open channel.</summary>
    public int HeldConfirms
    {
        get
        {
            lock (sync) return connections.SelectMany(c => c.Channels).Sum(ch => ch.Held.Count);
        }
    }

    /// <summary>Deliver every held confirm now, as <paramref name="outcome"/>: a broker confirming late.</summary>
    public int ReleaseHeldConfirms(ConfirmOutcome outcome)
    {
        var cb = new Callbacks();
        lock (sync)
            foreach (var ch in connections.SelectMany(c => c.Channels))
                while (ch.Held.TryDequeue(out var h))
                    cb.Event(events, () => h(outcome, ""));
        Post(cb);
        return cb.Count;
    }

    /// <summary>
    /// Close every channel consuming <paramref name="queue"/>, as a broker closes a channel over a
    /// consumer error, leaving the connection up.
    /// </summary>
    public void CloseChannelsConsuming(string queue, string reason)
    {
        var cb = new Callbacks();
        lock (sync)
        {
            if (!queues.TryGetValue(queue, out var q)) return;
            foreach (var ch in q.Consumers.Select(c => c.Channel).Distinct().ToList()) CloseChannel(ch, reason, cb);
        }
        Post(cb);
    }

    /// <summary>Cancel every consumer of <paramref name="queue"/> from the broker side (basic.cancel), leaving channels open.</summary>
    public void CancelConsumers(string queue)
    {
        var cb = new Callbacks();
        lock (sync)
        {
            if (!queues.TryGetValue(queue, out var q)) return;
            foreach (var c in q.Consumers.ToList())
            {
                c.Channel.Consumers.Remove(c.Tag);
                q.Consumers.Remove(c);
                if (c.OnCancel is { } onCancel) cb.Event(events, onCancel);
            }
        }
        Post(cb);
    }

    // ---- inspection --------------------------------------------------------------------

    public bool QueueExists(string name)
    {
        lock (sync) return queues.ContainsKey(name);
    }

    public bool ExchangeExists(string name)
    {
        lock (sync) return exchanges.ContainsKey(name);
    }

    /// <summary>Messages ready in the queue (not counting unacknowledged ones).</summary>
    public int QueueDepth(string name)
    {
        lock (sync) return queues.TryGetValue(name, out var q) ? q.Messages.Count : 0;
    }

    public int UnackedCount(string name)
    {
        lock (sync) return queues.TryGetValue(name, out var q) ? q.Unacked : 0;
    }

    public int ConsumerCount(string name)
    {
        lock (sync) return queues.TryGetValue(name, out var q) ? q.Consumers.Count : 0;
    }

    public IDictionary<string, object?>? QueueArguments(string name)
    {
        lock (sync) return queues.TryGetValue(name, out var q) ? new Dictionary<string, object?>(q.Arguments) : null;
    }

    /// <summary>The messages waiting in a queue, in delivery order, without removing them.</summary>
    public IReadOnlyList<Delivery> Peek(string name)
    {
        lock (sync)
        {
            if (!queues.TryGetValue(name, out var q)) return Array.Empty<Delivery>();
            return q.Messages
                .Select(m => new Delivery((byte[])m.Body.Clone(), m.Properties, m.Exchange, m.RoutingKey, "", 0, m.Redelivered))
                .ToList();
        }
    }

    /// <summary>The routing keys bound from <paramref name="exchange"/> to <paramref name="queue"/>.</summary>
    public IReadOnlyList<string> Bindings(string queue, string exchange)
    {
        lock (sync)
            return exchanges.TryGetValue(exchange, out var x)
                ? x.Bindings.Where(b => b.Queue == queue).Select(b => b.Key).ToList()
                : Array.Empty<string>();
    }

    public IReadOnlyList<string> QueueNames
    {
        get
        {
            lock (sync) return queues.Keys.ToList();
        }
    }

    public int OpenConnections
    {
        get
        {
            lock (sync) return connections.Count;
        }
    }

    public int ConnectAttempts
    {
        get
        {
            lock (sync) return connectAttempts;
        }
    }

    /// <summary>Wait until every callback queued so far has run: events, then each open channel's deliveries.</summary>
    public async Task FlushAsync()
    {
        await events.FlushAsync().ConfigureAwait(false);
        List<Lane> lanes;
        lock (sync) lanes = connections.SelectMany(c => c.Channels).Select(ch => ch.Deliveries).ToList();
        foreach (var lane in lanes) await lane.FlushAsync().ConfigureAwait(false);
        await events.FlushAsync().ConfigureAwait(false);
    }

    /// <summary>Wait until <paramref name="predicate"/> holds, polling, up to <paramref name="timeout"/> (default 5s).</summary>
    public static async Task<bool> WaitForAsync(Func<bool> predicate, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline) return false;
            await Task.Delay(2).ConfigureAwait(false);
        }
        return true;
    }

    /// <summary>Stop the broker. Connections are dropped first.</summary>
    public async ValueTask DisposeAsync()
    {
        KillConnections("broker shut down");
        await FlushAsync().ConfigureAwait(false);
        stopping.Cancel();
        events.Complete();
    }
}
