using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Protobus.Amqp;

/// <summary>
/// AMQP basic properties. Absent stays absent: nothing is written for a null property.
/// </summary>
public sealed record MessageProperties
{
    public string? ContentType { get; init; }
    public string? ContentEncoding { get; init; }
    public IDictionary<string, object?>? Headers { get; init; }
    /// <summary>2 = persistent.</summary>
    public byte? DeliveryMode { get; init; }
    public byte? Priority { get; init; }
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Expiration { get; init; }
    public string? MessageId { get; init; }
    /// <summary>Seconds since the epoch, as AMQP carries it.</summary>
    public long? Timestamp { get; init; }
    public string? Type { get; init; }
    public string? UserId { get; init; }
    public string? AppId { get; init; }
}

/// <summary>One message as the broker delivered it.</summary>
public sealed record Delivery(
    byte[] Body,
    MessageProperties Properties,
    string Exchange,
    string RoutingKey,
    string ConsumerTag,
    ulong DeliveryTag,
    bool Redelivered);

/// <summary>How the broker answered a publish on a confirm channel.</summary>
public enum ConfirmOutcome
{
    /// <summary>Stored.</summary>
    Ack,
    /// <summary>Refused (basic.nack).</summary>
    Nack,
    /// <summary>A mandatory publish matched no queue: returned, then acked.</summary>
    Returned,
    /// <summary>The channel closed with the publish unconfirmed: the outcome is UNKNOWN.</summary>
    Closed,
}

/// <summary>
/// A broker-reported failure: a channel or connection exception (<see cref="ReplyCode"/> 404,
/// 406, ...) or a client-side transport failure (0).
/// </summary>
public class AmqpException : ProtobusException
{
    public AmqpException(string message, int replyCode = 0, Exception? inner = null)
        : base(message, null, inner) => ReplyCode = replyCode;

    public int ReplyCode { get; }

    /// <summary>406 PRECONDITION_FAILED: a declare whose arguments disagree with the existing object's.</summary>
    public bool PreconditionFailed => ReplyCode == 406;
}

/// <summary>
/// One confirm-mode channel: the seam between protobus and an AMQP 0-9-1 client. The production
/// implementation is <see cref="RabbitTransport"/>; tests substitute
/// <see cref="Protobus.Testing.MemoryBroker"/>.
/// </summary>
/// <remarks>
/// Callbacks (deliveries, confirms, closes) run on the client's own threads and must not block;
/// deliveries to one consumer arrive one at a time. A channel-level failure closes the channel,
/// as AMQP specifies.
/// </remarks>
public interface IAmqpChannel
{
    Task DeclareExchangeAsync(string name, string type, bool durable, bool autoDelete, bool @internal,
        IDictionary<string, object?> arguments);

    /// <summary>Returns the queue's name: the given one, or the broker's for "".</summary>
    Task<string> DeclareQueueAsync(string name, bool durable, bool exclusive, bool autoDelete,
        IDictionary<string, object?> arguments);

    Task BindQueueAsync(string queue, string exchange, string routingKey);

    Task UnbindQueueAsync(string queue, string exchange, string routingKey);

    Task DeleteQueueAsync(string name);

    Task PurgeQueueAsync(string name);

    /// <summary>Per-consumer prefetch (basic.qos, global false).</summary>
    Task PrefetchAsync(ushort count);

    /// <summary>
    /// Start a consumer. <paramref name="onDelivery"/> is called one delivery at a time per
    /// consumer: the next is not delivered until the task it returns has completed, so it should
    /// return quickly unless order is the point. <paramref name="onCancel"/> runs when the broker
    /// cancels the consumer.
    /// </summary>
    Task<string> ConsumeAsync(string queue, string consumerTag, bool noAck, bool exclusive,
        Func<Delivery, Task> onDelivery, Action? onCancel);

    Task CancelAsync(string consumerTag);

    Task AckAsync(ulong deliveryTag);

    Task RejectAsync(ulong deliveryTag, bool requeue);

    /// <summary>
    /// Publish. <paramref name="onConfirm"/> is called exactly once, with the outcome and a detail
    /// string. Throws <see cref="AmqpException"/> only when nothing could be written, in which case
    /// <paramref name="onConfirm"/> is never called.
    /// </summary>
    Task PublishAsync(string exchange, string routingKey, byte[] body, MessageProperties properties, bool mandatory,
        Action<ConfirmOutcome, string> onConfirm);

    Task CloseAsync();

    bool IsOpen { get; }

    /// <summary>Called once when the channel closes for any reason; at once if it has.</summary>
    void OnClose(Action<string> listener);
}

/// <summary>One broker connection.</summary>
public interface IAmqpConnection
{
    Task<IAmqpChannel> OpenChannelAsync();

    /// <summary>A graceful close. The close listener then reports no error.</summary>
    Task CloseAsync();

    bool IsOpen { get; }

    /// <summary>Called once when the connection closes: with null after a close, the reason when lost.</summary>
    void OnClose(Action<string?> listener);
}

/// <summary>Opens connections.</summary>
public interface ITransport
{
    /// <summary>
    /// Connect and log in. <paramref name="heartbeatSeconds"/> applies unless the URL carries a
    /// <c>heartbeat</c> parameter, which wins (0 disables heartbeats).
    /// </summary>
    Task<IAmqpConnection> ConnectAsync(string url, int heartbeatSeconds);
}
