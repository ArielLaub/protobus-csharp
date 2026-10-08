using System;

namespace Protobus;

/// <summary>Options for a unary call or a one-way publish.</summary>
public sealed record CallOptions
{
    public static readonly CallOptions Default = new();

    /// <summary>Free-text caller identity, carried in the envelope for tracing. Not authenticated.</summary>
    public string? Actor { get; init; }

    /// <summary>false: publish without waiting for a reply; the call completes once the broker confirms it.</summary>
    public bool Rpc { get; init; } = true;

    /// <summary>
    /// How long the call may take; null for <see cref="Config.RpcCallTimeoutMs"/>. The deadline
    /// starts once the connection is ready and bounds the confirm as well as the reply.
    /// </summary>
    public long? TimeoutMs { get; init; }

    /// <summary>AMQP priority, 0-255; only a queue declared with a max priority honours it.</summary>
    public int? Priority { get; init; }

    /// <summary>
    /// The message's identity, as the consumer sees it; null for a fresh UUID. Set it to make a
    /// republish after an AMBIGUOUS failure recognisable: derive it from the work, never a clock.
    /// Refused when blank or longer than 255 bytes.
    /// </summary>
    public string? MessageId { get; init; }
}

/// <summary>Options for a streaming call.</summary>
public sealed record StreamOptions
{
    public static readonly StreamOptions Default = new();

    public string? Actor { get; init; }

    /// <summary>The longest gap between chunks; null for <see cref="Config.StreamIdleTimeoutMs"/>.</summary>
    public long? IdleTimeoutMs { get; init; }
}

/// <summary>
/// Retry for a service's requests: a failed request waits on <c>&lt;Service&gt;.Retry</c>, comes
/// back, and is dead-lettered to <c>&lt;Service&gt;.DLQ</c> once the hops are spent.
/// </summary>
/// <param name="MaxRetries">Retry hops before the DLQ; 0 disables retry.</param>
/// <param name="RetryDelayMs">The retry queue's <c>x-message-ttl</c>; fixed when first declared.</param>
/// <param name="MessageTtlMs">A TTL for the service's own queue, or null for none.</param>
public sealed record RetryOptions(int MaxRetries = 3, long RetryDelayMs = 5000, long? MessageTtlMs = null);

/// <summary>
/// Opt-in retry for event handlers. Without it a handler that throws loses its event. Enabled,
/// events climb the ladder requests do, and a retried event re-runs every matching handler.
/// </summary>
public sealed record EventRetryOptions(int MaxRetries = 0, long RetryDelayMs = 5000);

/// <summary>How a service consumes.</summary>
public sealed record MessageServiceOptions
{
    public static readonly MessageServiceOptions Default = new();

    /// <summary>Requests handled in parallel by this process: the queue's prefetch. Default 1.</summary>
    public int? MaxConcurrent { get; init; }

    public RetryOptions Retry { get; init; } = new();

    /// <summary>
    /// Ack after the handler completes (the default) rather than on delivery. Acking on delivery
    /// disables retry, dead-lettering and error replies.
    /// </summary>
    public bool LateAck { get; init; } = true;

    /// <summary>One attempt's limit; null for <see cref="Config.MessageProcessingTimeout"/>.</summary>
    public long? ProcessingTimeoutMs { get; init; }

    /// <summary>Declare the queue with this <c>x-max-priority</c>; null for a plain queue.</summary>
    public int? MaxPriority { get; init; }

    /// <summary>Retry for this service's event subscriptions; off by default.</summary>
    public EventRetryOptions EventRetry { get; init; } = new();
}

/// <summary>How a context connects.</summary>
public sealed record ContextOptions
{
    public static readonly ContextOptions Default = new();

    /// <summary>Reconnection backoff; null for the defaults.</summary>
    public ReconnectionOptions? Reconnection { get; init; }

    /// <summary>The AMQP transport; null for RabbitMQ. Tests pass a <see cref="Testing.MemoryBroker"/>.</summary>
    public Amqp.ITransport? Transport { get; init; }
}
