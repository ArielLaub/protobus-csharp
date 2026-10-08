using System.Collections.Generic;
using System.Threading;

namespace Protobus;

/// <summary>What a service handler receives besides its request.</summary>
public sealed class CallContext
{
    private readonly MessageHandlerContext delivery;

    internal CallContext(string actor, string correlationId, string method, MessageHandlerContext delivery)
    {
        Actor = actor;
        CorrelationId = correlationId;
        Method = method;
        this.delivery = delivery;
    }

    /// <summary>Free-text caller identity from the request envelope; empty when none. Not authenticated.</summary>
    public string Actor { get; }

    public string CorrelationId { get; }

    /// <summary>The contract method, <c>&lt;package&gt;.&lt;Service&gt;.&lt;method&gt;</c>.</summary>
    public string Method { get; }

    /// <summary>
    /// Fires on the processing timeout and, for a stream, when the caller cancels. Pass it to long
    /// or cancellable work: nothing else stops a handler.
    /// </summary>
    public CancellationToken CancellationToken => delivery.CancellationToken;

    /// <summary>The routing key the broker delivered on.</summary>
    public string RoutingKey => delivery.RoutingKey;

    /// <summary>Stable across redeliveries and retries: deduplicate on it. Null if the publisher set none.</summary>
    public string? MessageId => delivery.MessageId;

    public bool Redelivered => delivery.Redelivered;

    public IDictionary<string, object?> Headers => delivery.Headers;
}
