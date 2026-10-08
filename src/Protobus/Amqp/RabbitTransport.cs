using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Protobus.Amqp;

/// <summary>
/// The production transport, over RabbitMQ.Client 7.
/// </summary>
/// <remarks>
/// Supports <c>amqp://</c> and <c>amqps://</c> URLs with the RabbitMQ URI query parameters
/// <c>heartbeat</c>, <c>connection_timeout</c>, <c>channel_max</c> and <c>verify</c>. An
/// <c>amqps://</c> connection verifies the broker's certificate and host name; only
/// <c>verify=verify_none</c> turns that off, for development. A trailing <c>/</c> is the
/// default vhost, as in every port. The client's own automatic recovery is disabled: protobus
/// reconnects and restores its topology itself, the same way in every port.
/// </remarks>
public sealed class RabbitTransport : ITransport
{
    /// <summary>Tells two pending mandatory publishes sharing a messageId apart on a return.</summary>
    public const string PublishTagHeader = "x-protobus-publish-tag";

    public async Task<IAmqpConnection> ConnectAsync(string url, int heartbeatSeconds)
    {
        var factory = Configure(url, heartbeatSeconds);
        try
        {
            return new RabbitConnection(await factory.CreateConnectionAsync().ConfigureAwait(false));
        }
        catch (Exception e) when (e is not AmqpException)
        {
            throw Wrap("connect", e);
        }
    }

    /// <summary>The client's factory for a broker URL.</summary>
    internal static ConnectionFactory Configure(string url, int heartbeatSeconds)
    {
        var q = url.IndexOf('?');
        var @base = q < 0 ? url : url.Substring(0, q);
        var query = q < 0 ? new Dictionary<string, string>() : ParseQuery(url.Substring(q + 1));
        var factory = new ConnectionFactory();
        try
        {
            factory.Uri = new Uri(@base);
        }
        catch (Exception e)
        {
            // Never the input: an exception message about a URI can quote it, password included.
            throw new AmqpException($"invalid broker URL {Logger.RedactUrl(url)}: {e.GetType().Name}");
        }
        // The AMQP URI spec reads "amqp://host/" as the empty vhost. Every other protobus port
        // reaches the default vhost with it, so this one does too.
        if (string.IsNullOrEmpty(factory.VirtualHost)) factory.VirtualHost = "/";
        if (@base.StartsWith("amqps:", StringComparison.OrdinalIgnoreCase))
        {
            factory.Ssl.Enabled = true;
            factory.Ssl.ServerName = factory.HostName;
            if (string.Equals(query.GetValueOrDefault("verify"), "verify_none", StringComparison.OrdinalIgnoreCase))
            {
                Logger.Warn("amqps with verify=verify_none: the broker's certificate is NOT verified");
                factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch
                    | SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNotAvailable;
            }
            else
            {
                factory.Ssl.AcceptablePolicyErrors = SslPolicyErrors.None;
            }
        }
        factory.RequestedHeartbeat = TimeSpan.FromSeconds(IntParam(query, "heartbeat", heartbeatSeconds));
        if (query.ContainsKey("connection_timeout"))
            factory.RequestedConnectionTimeout = TimeSpan.FromMilliseconds(IntParam(query, "connection_timeout", 60000));
        if (query.ContainsKey("channel_max")) factory.RequestedChannelMax = (ushort)IntParam(query, "channel_max", 0);
        factory.AutomaticRecoveryEnabled = false;
        factory.TopologyRecoveryEnabled = false;
        factory.ClientProvidedName = "protobus-csharp";
        return factory;
    }

    private static Dictionary<string, string> ParseQuery(string query)
    {
        var out_ = new Dictionary<string, string>();
        foreach (var part in query.Split('&'))
        {
            if (part.Length == 0) continue;
            var eq = part.IndexOf('=');
            var k = Uri.UnescapeDataString(eq < 0 ? part : part.Substring(0, eq));
            var v = eq < 0 ? "" : Uri.UnescapeDataString(part.Substring(eq + 1));
            out_[k] = v;
        }
        return out_;
    }

    private static int IntParam(Dictionary<string, string> query, string key, int fallback)
    {
        if (!query.TryGetValue(key, out var v)) return fallback;
        if (int.TryParse(v.Trim(), out var n)) return n;
        throw new AmqpException($"invalid {key} in broker URL: {v}");
    }

    internal static AmqpException Wrap(string what, Exception e)
    {
        switch (e)
        {
            case AmqpException a:
                return a;
            case OperationInterruptedException oi when oi.ShutdownReason != null:
                return new AmqpException($"{what} failed: {oi.ShutdownReason.ReplyCode} {oi.ShutdownReason.ReplyText}",
                    oi.ShutdownReason.ReplyCode, e);
            case AlreadyClosedException ac when ac.ShutdownReason != null:
                return new AmqpException($"{what} failed: channel is closed ({ac.ShutdownReason.ReplyCode} {ac.ShutdownReason.ReplyText})",
                    504, e);
            case BrokerUnreachableException bu:
                return new AmqpException($"{what} failed: {Errors.MessageOf(bu.InnerException ?? bu)}", 0, e);
            default:
                return new AmqpException($"{what} failed: {Errors.MessageOf(e)}", 0, e);
        }
    }

    private static string Describe(ShutdownEventArgs? args) =>
        args == null ? "closed" : $"{args.ReplyCode} {args.ReplyText}";

    private static Action<T> Once<T>(Action<T> listener)
    {
        var fired = 0;
        return x =>
        {
            if (Interlocked.Exchange(ref fired, 1) == 0) listener(x);
        };
    }

    internal static IDictionary<string, object?>? ToArguments(IDictionary<string, object?> args) =>
        args.Count == 0 ? null : new Dictionary<string, object?>(args);

    private sealed class RabbitConnection : IAmqpConnection
    {
        private readonly IConnection connection;

        internal RabbitConnection(IConnection connection) => this.connection = connection;

        public async Task<IAmqpChannel> OpenChannelAsync()
        {
            try
            {
                var ch = await connection.CreateChannelAsync(new CreateChannelOptions(
                    publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: false)).ConfigureAwait(false);
                return new RabbitChannel(ch);
            }
            catch (Exception e)
            {
                throw Wrap("open channel", e);
            }
        }

        public async Task CloseAsync()
        {
            try
            {
                if (connection.IsOpen) await connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug("closing the connection: " + e.Message);
            }
        }

        public bool IsOpen => connection.IsOpen;

        public void OnClose(Action<string?> listener)
        {
            var once = Once(listener);
            connection.ConnectionShutdownAsync += (_, args) =>
            {
                once(args.Initiator == ShutdownInitiator.Application ? null : Describe(args));
                return Task.CompletedTask;
            };
            if (!connection.IsOpen)
            {
                var reason = connection.CloseReason;
                once(reason == null || reason.Initiator == ShutdownInitiator.Application ? null : Describe(reason));
            }
        }
    }

    private sealed class RabbitChannel : IAmqpChannel
    {
        private readonly IChannel ch;
        private readonly SemaphoreSlim publishLock = new(1, 1);
        private readonly ConcurrentDictionary<ulong, Pending> pending = new();

        private sealed class Pending
        {
            internal readonly string? MessageId;
            internal readonly string? Tag;
            internal readonly Action<ConfirmOutcome, string> OnConfirm;
            internal volatile bool Returned;

            internal Pending(string? messageId, string? tag, Action<ConfirmOutcome, string> onConfirm)
            {
                MessageId = messageId;
                Tag = tag;
                OnConfirm = onConfirm;
            }
        }

        internal RabbitChannel(IChannel ch)
        {
            this.ch = ch;
            ch.BasicAcksAsync += (_, e) =>
            {
                Settle(e.DeliveryTag, e.Multiple, false);
                return Task.CompletedTask;
            };
            ch.BasicNacksAsync += (_, e) =>
            {
                Settle(e.DeliveryTag, e.Multiple, true);
                return Task.CompletedTask;
            };
            ch.BasicReturnAsync += (_, e) =>
            {
                var id = e.BasicProperties.MessageId;
                var tag = Internal.Headers.Text(e.BasicProperties.Headers is { } h && h.TryGetValue(PublishTagHeader, out var t) ? t : null);
                // basic.return precedes the confirm of the same message, so the matching
                // publish is still pending here.
                foreach (var p in pending.OrderBy(kv => kv.Key).Select(kv => kv.Value))
                {
                    if (!p.Returned && p.MessageId == id && p.Tag == tag)
                    {
                        p.Returned = true;
                        break;
                    }
                }
                return Task.CompletedTask;
            };
            ch.ChannelShutdownAsync += (_, args) =>
            {
                var reason = Describe(args);
                foreach (var key in pending.Keys.ToArray())
                    if (pending.TryRemove(key, out var p)) Confirm(p, ConfirmOutcome.Closed, reason);
                return Task.CompletedTask;
            };
        }

        private void Settle(ulong seq, bool multiple, bool nack)
        {
            var done = new List<Pending>();
            if (multiple)
            {
                foreach (var key in pending.Keys.Where(k => k <= seq).OrderBy(k => k).ToArray())
                    if (pending.TryRemove(key, out var p)) done.Add(p);
            }
            else if (pending.TryRemove(seq, out var p))
            {
                done.Add(p);
            }
            foreach (var p in done)
            {
                var outcome = nack ? ConfirmOutcome.Nack : p.Returned ? ConfirmOutcome.Returned : ConfirmOutcome.Ack;
                Confirm(p, outcome, nack ? "basic.nack" : "");
            }
        }

        private static void Confirm(Pending p, ConfirmOutcome outcome, string detail)
        {
            try
            {
                p.OnConfirm(outcome, detail);
            }
            catch (Exception e)
            {
                Logger.Error("publish confirm callback failed: " + e);
            }
        }

        private async Task Run(string what, Func<Task> op)
        {
            try
            {
                await op().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw Wrap(what, e);
            }
        }

        public Task DeclareExchangeAsync(string name, string type, bool durable, bool autoDelete, bool @internal,
            IDictionary<string, object?> arguments) =>
            Run("exchange.declare " + name, () => ch.ExchangeDeclareAsync(name, type, durable, autoDelete, ToArguments(arguments), false, false));

        public async Task<string> DeclareQueueAsync(string name, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object?> arguments)
        {
            try
            {
                var ok = await ch.QueueDeclareAsync(name, durable, exclusive, autoDelete, ToArguments(arguments), false, false).ConfigureAwait(false);
                return ok.QueueName;
            }
            catch (Exception e)
            {
                throw Wrap("queue.declare " + name, e);
            }
        }

        public Task BindQueueAsync(string queue, string exchange, string routingKey) =>
            Run("queue.bind " + queue, () => ch.QueueBindAsync(queue, exchange, routingKey, null, false));

        public Task UnbindQueueAsync(string queue, string exchange, string routingKey) =>
            Run("queue.unbind " + queue, () => ch.QueueUnbindAsync(queue, exchange, routingKey, null));

        public Task DeleteQueueAsync(string name) => Run("queue.delete " + name, () => ch.QueueDeleteAsync(name, false, false, false));

        public Task PurgeQueueAsync(string name) => Run("queue.purge " + name, () => ch.QueuePurgeAsync(name));

        public Task PrefetchAsync(ushort count) => Run("basic.qos", () => ch.BasicQosAsync(0, count, false));

        public async Task<string> ConsumeAsync(string queue, string consumerTag, bool noAck, bool exclusive,
            Func<Delivery, Task> onDelivery, Action? onCancel)
        {
            var consumer = new Consumer(ch, queue, onDelivery, onCancel);
            try
            {
                return await ch.BasicConsumeAsync(queue, noAck, consumerTag, false, exclusive, null, consumer).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw Wrap("basic.consume " + queue, e);
            }
        }

        private sealed class Consumer : AsyncDefaultBasicConsumer
        {
            private readonly string queue;
            private readonly Func<Delivery, Task> onDelivery;
            private readonly Action? onCancel;

            internal Consumer(IChannel ch, string queue, Func<Delivery, Task> onDelivery, Action? onCancel) : base(ch)
            {
                this.queue = queue;
                this.onDelivery = onDelivery;
                this.onCancel = onCancel;
            }

            public override async Task HandleBasicDeliverAsync(string consumerTag, ulong deliveryTag, bool redelivered,
                string exchange, string routingKey, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body,
                CancellationToken cancellationToken = default)
            {
                try
                {
                    // The body's memory is the client's and is reused after this returns.
                    await onDelivery(new Delivery(body.ToArray(), FromClient(properties), exchange, routingKey,
                        consumerTag, deliveryTag, redelivered)).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // One bad delivery must not take the channel with it.
                    Logger.Error($"delivery callback failed on {queue}: {e}");
                }
            }

            protected override Task OnCancelAsync(string[] consumerTags, CancellationToken cancellationToken = default)
            {
                onCancel?.Invoke();
                return Task.CompletedTask;
            }
        }

        public Task CancelAsync(string consumerTag) => Run("basic.cancel", () => ch.BasicCancelAsync(consumerTag, false));

        public Task AckAsync(ulong deliveryTag) => Run("basic.ack", () => ch.BasicAckAsync(deliveryTag, false).AsTask());

        public Task RejectAsync(ulong deliveryTag, bool requeue) =>
            Run("basic.reject", () => ch.BasicRejectAsync(deliveryTag, requeue).AsTask());

        public async Task PublishAsync(string exchange, string routingKey, byte[] body, MessageProperties properties,
            bool mandatory, Action<ConfirmOutcome, string> onConfirm)
        {
            var tag = Internal.Headers.Text(Internal.Headers.Get(properties.Headers, PublishTagHeader));
            var props = ToClient(properties);
            await publishLock.WaitAsync().ConfigureAwait(false);
            ulong seq = 0;
            try
            {
                seq = await ch.GetNextPublishSequenceNumberAsync().ConfigureAwait(false);
                pending[seq] = new Pending(properties.MessageId, tag, onConfirm);
                await ch.BasicPublishAsync(exchange, routingKey, mandatory, props, body).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                pending.TryRemove(seq, out _);
                throw Wrap("basic.publish", e);
            }
            finally
            {
                publishLock.Release();
            }
        }

        public async Task CloseAsync()
        {
            try
            {
                if (ch.IsOpen) await ch.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug("closing a channel: " + e.Message);
            }
        }

        public bool IsOpen => ch.IsOpen;

        public void OnClose(Action<string> listener)
        {
            var once = Once(listener);
            ch.ChannelShutdownAsync += (_, args) =>
            {
                once(Describe(args));
                return Task.CompletedTask;
            };
            if (!ch.IsOpen) once(Describe(ch.CloseReason));
        }
    }

    internal static BasicProperties ToClient(MessageProperties p)
    {
        var b = new BasicProperties();
        if (p.ContentType != null) b.ContentType = p.ContentType;
        if (p.ContentEncoding != null) b.ContentEncoding = p.ContentEncoding;
        if (p.Headers != null) b.Headers = new Dictionary<string, object?>(p.Headers);
        if (p.DeliveryMode != null) b.DeliveryMode = (DeliveryModes)p.DeliveryMode.Value;
        if (p.Priority != null) b.Priority = p.Priority.Value;
        if (p.CorrelationId != null) b.CorrelationId = p.CorrelationId;
        if (p.ReplyTo != null) b.ReplyTo = p.ReplyTo;
        if (p.Expiration != null) b.Expiration = p.Expiration;
        if (p.MessageId != null) b.MessageId = p.MessageId;
        if (p.Timestamp != null) b.Timestamp = new AmqpTimestamp(p.Timestamp.Value);
        if (p.Type != null) b.Type = p.Type;
        if (p.UserId != null) b.UserId = p.UserId;
        if (p.AppId != null) b.AppId = p.AppId;
        return b;
    }

    internal static MessageProperties FromClient(IReadOnlyBasicProperties p) => new()
    {
        ContentType = p.IsContentTypePresent() ? p.ContentType : null,
        ContentEncoding = p.IsContentEncodingPresent() ? p.ContentEncoding : null,
        Headers = p.IsHeadersPresent() && p.Headers != null ? new Dictionary<string, object?>(p.Headers) : null,
        DeliveryMode = p.IsDeliveryModePresent() ? (byte)p.DeliveryMode : null,
        Priority = p.IsPriorityPresent() ? p.Priority : null,
        CorrelationId = p.IsCorrelationIdPresent() ? p.CorrelationId : null,
        ReplyTo = p.IsReplyToPresent() ? p.ReplyTo : null,
        Expiration = p.IsExpirationPresent() ? p.Expiration : null,
        MessageId = p.IsMessageIdPresent() ? p.MessageId : null,
        Timestamp = p.IsTimestampPresent() ? p.Timestamp.UnixTime : null,
        Type = p.IsTypePresent() ? p.Type : null,
        UserId = p.IsUserIdPresent() ? p.UserId : null,
        AppId = p.IsAppIdPresent() ? p.AppId : null,
    };
}
