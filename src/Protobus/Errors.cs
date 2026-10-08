using System;

namespace Protobus;

/// <summary>
/// The base of every error protobus raises. <see cref="Code"/> is the stable, cross-language
/// identifier of the failure (<c>RPC_TIMEOUT</c>, <c>UNROUTABLE</c>, a service's own
/// <c>VALIDATION_ERROR</c>, ...), or null for errors that carry none.
/// </summary>
public class ProtobusException : Exception
{
    public ProtobusException(string? message, string? code = null, Exception? inner = null)
        : base(message, inner) => Code = code;

    public string? Code { get; }
}

/// <summary>
/// An expected failure that is answered, never retried. The caller receives its message and
/// code, and the request is settled. Use it for validation and business-rule failures.
/// </summary>
public class HandledError : ProtobusException
{
    public HandledError(string message, string? code = null, Exception? inner = null)
        : base(message, string.IsNullOrEmpty(code) ? "HANDLED_ERROR" : code, inner) { }
}

/// <summary>
/// The message could not be understood: it did not decode, or it named something this service
/// does not serve. Handled by definition: it fails the same way on every redelivery.
/// </summary>
public class ProtocolError : HandledError
{
    public ProtocolError(string message) : base(message, "PROTOCOL_ERROR") { }
}

/// <summary>The request named a method this service does not serve, or one its routing key contradicts.</summary>
public class InvalidMethodError : ProtocolError
{
    public InvalidMethodError(string message) : base(message) { }
}

/// <summary>Substituted for an unhandled service error before it reaches the caller, when exposure is off.</summary>
public class InternalServiceError : ProtobusException
{
    public InternalServiceError(string? correlationId)
        : base(string.IsNullOrEmpty(correlationId)
            ? "internal service error"
            : $"internal service error (correlationId {correlationId})", "INTERNAL_ERROR") { }
}

/// <summary>
/// An error a service answered with, as its caller receives it. Only the message, the code and
/// the method cross the wire.
/// </summary>
public class RemoteError : ProtobusException
{
    public RemoteError(string message, string? code, string method)
        : base(message, string.IsNullOrEmpty(code) ? null : code) => Method = method;

    /// <summary>The method the error was reported against.</summary>
    public string Method { get; }
}

/// <summary>A unary call got no reply within its timeout.</summary>
public class RpcTimeoutError : ProtobusException
{
    public RpcTimeoutError(string message) : base(message, "RPC_TIMEOUT") { }
}

/// <summary>
/// A handler exceeded its processing timeout. Retried like an unhandled error; the caller is
/// answered with this code once the retries are spent. The handler's cancellation token fires.
/// </summary>
public class TimeoutError : ProtobusException
{
    public TimeoutError(string message) : base(message, "PROCESSING_TIMEOUT") { }
}

/// <summary>
/// A publish that did not demonstrably reach a queue. <see cref="PublishConfirmTimeoutError"/>
/// and <see cref="ChannelClosedError"/> are AMBIGUOUS (the broker may have stored it).
/// </summary>
public class PublishError : ProtobusException
{
    public PublishError(string message, string code, string? messageId) : base(message, code) => MessageId = messageId;

    /// <summary>The messageId the publish carried.</summary>
    public string? MessageId { get; }
}

/// <summary>The broker refused the message (basic.nack): definitely not stored, safe to republish.</summary>
public class PublishNackedError : PublishError
{
    public PublishNackedError(string message, string? messageId) : base(message, "PUBLISH_NACKED", messageId) { }
}

/// <summary>A mandatory publish matched no queue and was returned.</summary>
public class UnroutableError : PublishError
{
    public UnroutableError(string message, string? messageId) : base(message, "UNROUTABLE", messageId) { }
}

/// <summary>
/// No confirm in time (or no confirm slot came free, in which case it was not sent). The outcome
/// is UNKNOWN: republish with the same messageId.
/// </summary>
public class PublishConfirmTimeoutError : PublishError
{
    public PublishConfirmTimeoutError(string message, string? messageId) : base(message, "PUBLISH_CONFIRM_TIMEOUT", messageId) { }
}

/// <summary>The channel closed before the confirm: an AMBIGUOUS outcome.</summary>
public class ChannelClosedError : PublishError
{
    public ChannelClosedError(string message, string? messageId) : base(message, "CHANNEL_CLOSED", messageId) { }
}

/// <summary>The base of the failures a streaming call raises on the caller's side.</summary>
public class StreamingError : ProtobusException
{
    public StreamingError(string message) : base(message) { }
}

/// <summary>No chunk within the idle timeout.</summary>
public class StreamTimeoutError : StreamingError
{
    public StreamTimeoutError(string message) : base(message) { }
}

/// <summary>The consumer fell behind the producer past a buffer bound.</summary>
public class StreamBackpressureError : StreamingError
{
    public StreamBackpressureError(string message) : base(message) { }
}

/// <summary>A chunk was lost: a gap in the sequence numbers.</summary>
public class StreamSequenceError : StreamingError
{
    public StreamSequenceError(string message) : base(message) { }
}

/// <summary>The connection is not carrying traffic, and did not come back in time. Nothing was attempted.</summary>
public class NotReadyError : ProtobusException
{
    public NotReadyError(string message) : base(message, "NOT_READY") { }
}

/// <summary>A reconnection attempt failed, or the connection gave up reconnecting.</summary>
public class ReconnectionError : ProtobusException
{
    public ReconnectionError(string message) : base(message) { }
}

/// <summary>Connect was called on a connection that is already connected.</summary>
public class AlreadyConnectedError : ProtobusException
{
    public AlreadyConnectedError() : base("already connected") { }
}

/// <summary>The connection was lost, or the context closed, while a call or stream was pending.</summary>
public class DisconnectedError : ProtobusException
{
    public DisconnectedError(string message = "Connection lost during RPC call") : base(message) { }
}

/// <summary>Something that needs a connection was attempted without one, and none is being re-established.</summary>
public class NotConnectedError : ProtobusException
{
    public NotConnectedError(string message = "not connected") : base(message) { }
}

/// <summary>A component was used before its initialisation, or initialised twice.</summary>
public class NotInitializedError : ProtobusException
{
    public NotInitializedError(string message = "not initialized") : base(message) { }
}

/// <summary><c>Init</c> was called a second time.</summary>
public class AlreadyInitializedError : ProtobusException
{
    public AlreadyInitializedError(string message = "already initialized") : base(message) { }
}

/// <summary>A listener already consuming was started again.</summary>
public class AlreadyStartedError : ProtobusException
{
    public AlreadyStartedError() : base("already started") { }
}

/// <summary>A <c>maxPriority</c> or priority AMQP cannot carry, refused before anything reaches the broker.</summary>
public class InvalidPriorityError : ProtobusException
{
    public InvalidPriorityError(string message) : base(message) { }
}

/// <summary>A caller-supplied messageId that cannot identify anything: blank, or over 255 bytes.</summary>
public class InvalidMessageIdError : ProtobusException
{
    public InvalidMessageIdError(string message) : base(message) { }
}

/// <summary>The retry queue exists with other arguments, in practice a changed retry delay.</summary>
public class RetryQueueMismatchError : ProtobusException
{
    public RetryQueueMismatchError(string message) : base(message) { }
}

/// <summary>No schema declares the service a class serves.</summary>
public class MissingProtoError : ProtobusException
{
    public MissingProtoError(string message) : base(message) { }
}

/// <summary>No known service matches a proxy's name, or any prefix of it.</summary>
public class InvalidServiceNameError : ProtobusException
{
    public InvalidServiceNameError(string message) : base(message) { }
}

/// <summary>A request could not be encoded.</summary>
public class InvalidRequestError : ProtobusException
{
    public InvalidRequestError(string message) : base(message) { }
}

/// <summary>A reply could not be decoded.</summary>
public class InvalidResponseError : ProtobusException
{
    public InvalidResponseError(string message) : base(message) { }
}

/// <summary>A handler returned something not of its method's response type.</summary>
public class InvalidResultError : ProtobusException
{
    public InvalidResultError(string message) : base(message) { }
}

/// <summary>An event could not be encoded.</summary>
public class InvalidMessageError : ProtobusException
{
    public InvalidMessageError(string message) : base(message) { }
}

/// <summary>A name that is not <c>&lt;package&gt;.&lt;Service&gt;.&lt;method&gt;</c>.</summary>
public class InvalidMethodNameError : ProtobusException
{
    public InvalidMethodNameError(string message) : base(message) { }
}

/// <summary>A well-formed method name the named service does not declare.</summary>
public class UnknownMethodError : ProtobusException
{
    public UnknownMethodError(string message) : base(message) { }
}

/// <summary>A schema could not be loaded, or a type does not resolve.</summary>
public class SchemaError : ProtobusException
{
    public SchemaError(string message, Exception? inner = null) : base(message, null, inner) { }
}

/// <summary>
/// A custom-type value outside what the wire format carries: a negative or oversized bigint, one
/// wider than 32 bytes, or a timestamp beyond ±8.64e15 ms.
/// </summary>
public class CustomTypeRangeError : ProtobusException
{
    public CustomTypeRangeError(string message) : base(message) { }
}

/// <summary>How errors are classified, and what of them may leave the process.</summary>
public static class Errors
{
    /// <summary>True for an expected failure that is answered rather than retried.</summary>
    public static bool IsHandledError(Exception? error) => error is HandledError;

    public static string? CodeOf(Exception? error) => (error as ProtobusException)?.Code;

    /// <summary>The error's name as it appears in logs and retry headers: its class name.</summary>
    public static string NameOf(Exception? error) => error == null ? "UnknownError" : error.GetType().Name;

    /// <summary>
    /// What an error looks like to the caller. A HandledError and a processing timeout pass
    /// through; anything else becomes an <see cref="InternalServiceError"/> unless exposure is on.
    /// </summary>
    public static Exception SanitizeErrorForClient(Exception error, string? correlationId)
    {
        if (IsHandledError(error) || error is TimeoutError) return error;
        if (Config.ExposeInternalErrors) return error;
        return new InternalServiceError(correlationId);
    }

    /// <summary>
    /// A non-disclosing description: name and code, never the message, except for a HandledError,
    /// which is meant to be seen. It travels in the <c>x-last-error</c> header.
    /// </summary>
    public static string SafeErrorSummary(Exception? error)
    {
        if (error == null) return "UnknownError";
        var name = NameOf(error);
        var code = CodeOf(error);
        if (IsHandledError(error)) return $"{name}[{code}]: {error.Message}";
        return string.IsNullOrEmpty(code) ? name : $"{name}[{code}]";
    }

    internal static string MessageOf(Exception? error) =>
        error == null ? "unknown error" : string.IsNullOrEmpty(error.Message) ? NameOf(error) : error.Message;

    /// <summary>The innermost error of an aggregate, as it was raised.</summary>
    internal static Exception Unwrap(Exception e)
    {
        while (e is AggregateException { InnerExceptions.Count: 1 } a) e = a.InnerExceptions[0];
        return e;
    }
}
