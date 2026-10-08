# Errors

## On the service

A handler's exception decides what happens to the request.

**`HandledError`** is an answer. The caller receives its message and code
(`HANDLED_ERROR` unless you give one), and the request is acknowledged. Use it,
or a subclass, for validation and business-rule failures:

<!-- doc-check: compile -->
```csharp
using Protobus;

public class ValidationError : HandledError
{
    public ValidationError(string message) : base(message, "VALIDATION_ERROR") { }
}
```

**Anything else** is a failure. The request is published to the service's
retry exchange, waits `RetryDelayMs` on `<Service>.Retry`, and comes back
through the bus. After `MaxRetries` (default 3) it is dead-lettered to
`<Service>.DLQ`, and only then is the caller answered with the error. A
processing timeout counts as a failure and is answered `PROCESSING_TIMEOUT`.

**A message that cannot be understood** (an envelope or payload that does not
decode, a method the contract does not declare, a routing key that contradicts
the body, an rpc the service did not implement) is answered at once with
`PROTOCOL_ERROR` and never retried: the same bytes would fail the same way
every time.

## Retry and dead-letter copies

The retry and DLQ copies keep the original body, `messageId`, `correlationId`,
content type and encoding, priority, timestamp, type and app id, and add:

| Header | |
|---|---|
| `x-retry-count` | hops so far |
| `x-original-routing-key` | the routing key it was delivered on |
| `x-first-failure-time` | milliseconds since the epoch |
| `x-last-error` | the error's type name and code (and, for a `HandledError`, its message) |
| `x-original-queue` | on the DLQ copy: the queue it failed on |
| `x-dlq-time` | on the DLQ copy: when it was dead-lettered |

`x-last-error` never carries an unhandled error's message: messages routinely
quote the data that caused them, and these headers outlive the process in a
queue operators browse.

## What the caller sees

A service error is a `RemoteError` with the code and message the service sent,
and the `Method` it was reported against:

| Code | |
|---|---|
| `HANDLED_ERROR` | a `HandledError` without a code of its own |
| your own | a `HandledError` subclass's code |
| `PROTOCOL_ERROR` | the request could not be served as sent |
| `PROCESSING_TIMEOUT` | every attempt exceeded the processing timeout |
| `INTERNAL_ERROR` | an unhandled error, when `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false` |

An unhandled error's message reaches the caller by default: the caller is
another of your services, inside the same trust boundary. A service whose
callers relay errors to untrusted clients should set
`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`; callers then get
`internal service error (correlationId ...)` and the real error stays in the
service's log. A `HandledError` always crosses.

Delivery failures are separate types, with their own retry guidance: see
[Clients](clients.md#what-a-call-can-throw).
