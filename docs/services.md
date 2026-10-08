# Services

A service extends the generated `<Service>Protobus.Base` and overrides its
rpcs. Each handler receives the decoded request and a `CallContext`, and
returns the response (or, for a streaming rpc, an `IAsyncEnumerable` of
chunks; see [Streaming](streaming.md)).

```csharp
public override Task<AddResponse> Add(AddRequest request, CallContext context) { ... }
```

What a handler throws decides the outcome:

| Thrown | Caller receives | Request |
|---|---|---|
| nothing | the response | acknowledged |
| `HandledError` (or a subclass) | its message and code, at once | acknowledged, never retried |
| anything else | the final error, once retries are spent | retried, then dead-lettered |

See [Errors](errors.md).

## Starting and stopping

`RunnableService.StartAsync(context, constructor)` constructs the service,
calls `InitAsync()` and registers it for graceful shutdown, which runs on
SIGINT or SIGTERM or on `RunnableService.RequestShutdown()`:

1. every started service stops consuming, keeping its channels open so the
   work in hand can still reply and acknowledge;
2. in-flight deliveries drain, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` (30 s);
3. each service's `CleanupAsync()` runs;
4. the contexts are disposed.

`RunnableService.WaitForShutdownAsync()` completes when that has happened, with
the exit code. If `InitAsync()` fails, `StartAsync` disposes the service and
the context and rethrows.

A service can also be driven by hand, for example from a generic host's
`IHostedService`: construct it, `await InitAsync()`, and at shutdown
`await StopConsumingAsync()`, `await context.Connection.DrainInFlightAsync(ms)`,
then `await context.DisposeAsync()`.

## Options

<!-- doc-check: compile -->
```csharp
using Protobus;

public static class ServiceOptions
{
    public static readonly MessageServiceOptions Options = new()
    {
        // Requests handled in parallel by this process: the queue's prefetch. Default 1.
        MaxConcurrent = 16,
        // Retry hops before the DLQ (default 3), and the delay between them (default 5000 ms).
        Retry = new RetryOptions(MaxRetries: 5, RetryDelayMs: 10000),
        // One attempt's time limit (default MESSAGE_PROCESSING_TIMEOUT, 10 minutes).
        ProcessingTimeoutMs = 30000,
        // Declare the queue with x-max-priority (default: a plain queue).
        MaxPriority = Config.RecommendedMaxPriority,
        // Retry for this service's event subscriptions (default: off).
        EventRetry = new EventRetryOptions(MaxRetries: 3),
    };
}
```

`LateAck = false` acknowledges on delivery instead of after the handler. It
turns off retries, dead-lettering and, for priority queues, prefetch: use it
only for genuine at-most-once work. `MaxPriority` together with
`LateAck = false` is refused.

RabbitMQ fixes a queue's arguments when it is first declared. Changing
`RetryDelayMs` for a service that has run fails `InitAsync()` with
`RetryQueueMismatchError`; changing `MaxPriority` or `MessageTtlMs` fails with
the broker's 406. Drain and delete the queue to change them.

## Concurrency

Handlers run on the .NET thread pool, up to `MaxConcurrent` at once per
service, so a handler must be safe for concurrent use. Await rather than block:
a handler that blocks a pool thread holds it for every other handler in the
process. Event handlers run the same way, bounded by `DEFAULT_PREFETCH`.

## The call context

| | |
|---|---|
| `Actor` | the caller's free-text identity, from the request envelope. Not authenticated. |
| `CorrelationId` | the call's id |
| `Method` | the contract method, `Calculator.Service.add` |
| `MessageId` | stable across redeliveries and retries: deduplicate on it |
| `Redelivered` | the broker has delivered this message before |
| `RoutingKey` | the key the broker delivered on |
| `Headers` | the delivery's AMQP headers |
| `CancellationToken` | fires on the processing timeout and, for a stream, on cancellation |

protobus does not abort a handler. When the processing timeout passes, the
token fires, the attempt is failed and retried, and the result the handler
eventually returns is discarded. Pass the token to the work: `Task.Delay`,
`HttpClient`, a database driver.

## Processing timeouts

`ProcessingTimeoutMs` caps one attempt at a unary request. A timed-out attempt
is retried like an unhandled error, and once the retries are spent the caller
is answered with `PROCESSING_TIMEOUT`. A streaming rpc is not bounded by it:
its caller's idle timeout applies instead (see [Streaming](streaming.md)).

## Instance names

Several services can serve one contract under their own names: override
`ServiceName`.

<!-- doc-check: compile -->
```csharp
using Combat;
using Protobus;

public class Player : PlayerProtobus.Base
{
    private readonly string id;

    public Player(Context context, string id) : base(context) => this.id = id;

    // Its own queue, bound to REQUEST.Combat.Player.<id>.*
    public override string ServiceName => "Combat.Player." + id;
}
```

The contract is found by trimming segments off the name until one names a
service in the schema. A proxy built with the instance name reaches that
instance: `new PlayerProtobus.Proxy(context, "Combat.Player.player6")`.

## Without generated code

Extend `MessageService` directly, return the schema's `FileDescriptor` from
`Schema` (or register it with `context.Factory.Register`, or load it from a
descriptor set; see [Clients](clients.md#the-untyped-proxy)), and register
handlers, typed with a message's parser or over serialized payloads:

<!-- doc-check: compile -->
```csharp
using System.Threading.Tasks;
using Calculator;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Protobus;

public class HandWritten : MessageService
{
    public HandWritten(Context context) : base(context)
    {
        RegisterUnary<AddRequest, AddResponse>("add", AddRequest.Parser,
            (request, call) => Task.FromResult(new AddResponse { Result = request.A + request.B }));
        // Serialized bytes in and out: nothing checks the reply's type.
        RegisterMethod("divide", (payload, call) =>
        {
            var request = DivideRequest.Parser.ParseFrom(payload);
            return Task.FromResult(new DivideResponse { Quotient = request.Dividend / request.Divisor }.ToByteArray());
        });
    }

    public override string ServiceName => "Calculator.Service";

    protected override FileDescriptor? Schema => CalculatorReflection.Descriptor;
}
```

Google.Protobuf for C# has no dynamic messages, so the untyped form is
serialized bytes rather than a message built from a descriptor.

## Events

A service publishes with `PublishEventAsync(message)` and subscribes with
`SubscribeEventAsync<T>(handler)`, after `InitAsync()`. See [Events](events.md).
