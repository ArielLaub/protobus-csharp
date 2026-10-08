# ProtoBus for .NET

**RabbitMQ-native microservices for C# and .NET, with Protocol Buffers on the wire.**

[![CI](https://github.com/ArielLaub/protobus-csharp/actions/workflows/ci.yml/badge.svg)](https://github.com/ArielLaub/protobus-csharp/actions/workflows/ci.yml)
[![.NET](https://img.shields.io/badge/.NET-8%2B-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-%E2%89%A53.8-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com)

Define a service in a `.proto` file, implement the class protobus generates for
it, and call it from anywhere on the bus as if it were local. ProtoBus turns
each service into **one durable RabbitMQ queue with N processes competing for
it**, so load balancing, failover, backpressure, retries and dead-lettering are
handled by the broker.

This is the C# port of [protobus](https://github.com/ArielLaub/protobus)
(TypeScript), [protobus-py](https://github.com/ArielLaub/protobus-py) (Python),
[protobus-go](https://github.com/ArielLaub/protobus-go) (Go),
[protobus-cpp](https://github.com/ArielLaub/protobus-cpp) (C++) and
[protobus-java](https://github.com/ArielLaub/protobus-java) (Java), designed
after the TypeScript reference class for class. The six are
**wire-compatible**: a C# service serves TypeScript, Python, Go, C++ and Java
callers and the other way round, with streaming, events, custom types and
error codes included. See [Compatibility](docs/compatibility.md).

**Status: new.** 2.0.0 is the first release of the C# port. Its runtime is at
feature parity with the TypeScript port's, and its test suites run it against
the TypeScript, Python, Go, C++ and Java ports' real libraries, in both
directions, on RabbitMQ 3 and 4.

---

## Install

.NET 8 or newer.

```bash
dotnet add package Protobus
dotnet add package Protobus.Tools
```

`Protobus` is the runtime. `Protobus.Tools` generates the code at build time
for the schemas you point it at; protoc comes with it (from Grpc.Tools), so
nothing needs installing:

```xml
<ItemGroup>
  <ProtobusSchemas Include="schemas" />
</ItemGroup>
```

For the command-line generator and the protoc plugin, see
[Code generation](docs/codegen.md). The runtime depends on
[Google.Protobuf](https://www.nuget.org/packages/Google.Protobuf) and
[RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client) 7, and on
nothing else.

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Quick start

Four steps to a working RPC. The code is the
[`examples`](examples/Calculator.cs) calculator, trimmed.

### 1. Describe the service

```protobuf
// schemas/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}
```

The package plus the service name is the service's name on the bus:
`Calculator.Service`. The file is an ordinary protobus schema, shared as-is
with the other languages. It needs no C# options.

### 2. Generate the C# code

`Protobus.Tools` runs before every compile. The proto package becomes the
namespace, PascalCased as protoc does (`package Calculator` stays
`Calculator`), holding protoc's message classes plus `ServiceProtobus`, with:

- `ServiceProtobus.Base`, the class your implementation extends, with a
  virtual method per rpc (`Add`, `Divide`);
- `ServiceProtobus.Proxy`, a typed client (`AddAsync`, `DivideAsync`).

### 3. Implement and run the service

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public class CalculatorService : ServiceProtobus.Base
{
    public CalculatorService(Context context, MessageServiceOptions? options) : base(context, options) { }

    public override Task<AddResponse> Add(AddRequest request, CallContext context) =>
        Task.FromResult(new AddResponse { Result = request.A + request.B });

    public override Task<DivideResponse> Divide(DivideRequest request, CallContext context)
    {
        // A HandledError is an answer, not a failure: never retried.
        if (request.Divisor == 0) throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        return Task.FromResult(new DivideResponse { Quotient = request.Dividend / request.Divisor });
    }

    public static async Task<int> Main()
    {
        var context = new Context();
        await context.InitAsync(Environment.GetEnvironmentVariable("AMQP_URL")!);
        await RunnableService.StartAsync(context, (c, o) => new CalculatorService(c, o),
            new MessageServiceOptions { MaxConcurrent = 8 });
        // Serves until SIGINT/SIGTERM, then drains in-flight work and closes.
        return await RunnableService.WaitForShutdownAsync();
    }
}
```

Every handler receives a `CallContext` with the caller's actor, the message id
to deduplicate on, and a `CancellationToken` that fires when the processing
timeout expires. An rpc you do not override answers `PROTOCOL_ERROR`.

### 4. Call it

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public static class Client
{
    public static async Task Main()
    {
        await using var context = new Context();
        await context.InitAsync(Environment.GetEnvironmentVariable("AMQP_URL")!);
        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();
        var sum = await calculator.AddAsync(new AddRequest { A = 5, B = 3 }, new CallOptions { TimeoutMs = 10000 });
        Console.WriteLine($"5 + 3 = {sum.Result}");
    }
}
```

```
$ dotnet run --project service &
$ dotnet run --project client
5 + 3 = 8
```

Without `TimeoutMs` the call is bounded by `RPC_CALL_TIMEOUT_MS` (10 minutes
by default, as in the other ports). Every proxy method also takes a
`CancellationToken`.

The full walkthrough adds events, error handling and a unit test:
**[Getting Started](docs/getting-started.md)**.

---

## Why ProtoBus

### RabbitMQ only, on purpose

ProtoBus is built for one broker, so the things a broker is good at stay in the
broker instead of being reimplemented above it:

| Concern | Where it lives |
|---|---|
| Load balancing | competing consumers on one queue |
| Routing | topic exchange bindings (`REQUEST.<Service>.*`) |
| Redelivery on consumer loss | late ack: an unacked delivery returns to the queue |
| Retry delay | the retry queue's `x-message-ttl`, drained by a dead-letter exchange |
| Persistence | durable queues, persistent messages |
| Dead letters | a real `<Service>.DLQ` |
| Priority | native queue priorities |

A request goes publisher → exchange → queue → consumer. Nothing tracks live
instances, so nothing holds a stale one, and a consumer that dies mid-request
leaves its delivery unacked for the next consumer to take.

If you may need to swap RabbitMQ for another broker, use a transport-agnostic
framework instead. That is a real feature and protobus does not have it.

### Protocol Buffers, not JSON

- **Contract-first.** The `.proto` file is the interface between teams and
  languages, and the generated C# types fail the build when the two drift
  apart.
- **Versioning by field number.** Adding a field does not break an old peer.

### A small protocol, a small port

The protocol protobus adds on top of AMQP is small and documented: five
envelope messages, a routing-key convention, an error encoding and two
streaming headers. Because queueing, consumer distribution and retry delays
belong to the broker, a port adapts that protocol to another AMQP client
rather than reimplementing messaging. This port depends on RabbitMQ.Client for
AMQP and Google.Protobuf for the wire, and nothing else at runtime. It brings
no container, no dependency injection and no reflection-based dispatch: the
generated code registers each rpc's handler directly.

---

## Features

- **Retries and dead-lettering.** An unhandled exception or a processing
  timeout parks the request on `<Service>.Retry` and redelivers it; after
  `MaxRetries` (3 by default, 5 seconds apart) it lands on `<Service>.DLQ`,
  and the caller is answered with the final error. A `HandledError` is an
  answer and is never retried. See [Errors](docs/errors.md).
- **Server streaming.** A method declared `returns (stream T)` is implemented
  as an `IAsyncEnumerable<T>` and consumed with `await foreach`. Leaving the
  loop early, or cancelling its token, stops the producer on the server. See
  [Streaming](docs/streaming.md).
- **Events.** `PublishEventAsync` publishes on a topic exchange;
  `SubscribeEventAsync<T>` receives events by message type, topic pattern
  (`*`, `#`) or both, with optional per-listener retries and a dead-letter
  queue. See [Events](docs/events.md).
- **Custom types.** `bigint` (unsigned 256-bit) and `timestamp` (milliseconds
  since the epoch) are built in and need no import in the schema; in C# they
  convert to `BigInteger` and `DateTimeOffset` with `CustomTypes`. Declare
  your own as `NAME:WIRE`. See [Code generation](docs/codegen.md).
- **Priority.** `MaxPriority` makes a service queue a priority queue, and
  `CallOptions.Priority` lets a control message overtake a bulk backlog.
- **Instance-named services.** Many instances can serve one contract under
  their own names (`Combat.Player.player6`), each addressed by its own proxy.
- **Processing timeout.** `ProcessingTimeoutMs` caps one attempt at a unary
  request; the handler's token fires and the attempt counts as failed.
- **Reconnection.** A lost connection is re-established with capped
  exponential backoff, and every service, listener and dispatcher is restored
  before the connection reports itself ready. Calls made meanwhile wait for it;
  calls in flight when it dropped fail with `DisconnectedError`.
- **Publisher confirms.** Every publish waits for the broker's confirm. A
  failure is a `PublishError` that says whether the outcome is ambiguous, and
  `CallOptions.MessageId` makes a republish safe to deduplicate.
- **Async throughout.** Handlers return `Task`, streams are
  `IAsyncEnumerable`, and cancellation is a `CancellationToken`; nothing blocks
  a thread.
- **Untyped calls.** `ServiceProxy.CallAsync` with a `MessageParser`, and
  `CallRawAsync` / `RegisterMethod` over serialized payloads, serve and call
  services without generated proxies.
- **An in-memory broker.** `MemoryBroker` runs services and clients
  in-process, with retries, dead-lettering, priorities and connection loss
  modelled. See [Testing](docs/testing.md).
- **Graceful shutdown.** `RunnableService` stops intake on SIGINT/SIGTERM, lets
  in-flight work finish within `SHUTDOWN_DRAIN_TIMEOUT_MS`, runs your
  `CleanupAsync()`, then closes.

### A short tour

<!-- doc-check: compile -->
```csharp
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Calculator;
using Chat;
using Protobus;

public static class Tour
{
    public static async Task RunAsync(Context context)
    {
        // Options on a service: concurrency, retries, a processing timeout and a priority queue.
        var options = new MessageServiceOptions
        {
            MaxConcurrent = 16,
            Retry = new RetryOptions(MaxRetries: 5, RetryDelayMs: 10000),
            ProcessingTimeoutMs = 30000,
            MaxPriority = Config.RecommendedMaxPriority,
        };

        // Options on a call.
        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();
        await calculator.AddAsync(new AddRequest(), new CallOptions
        {
            Priority = Config.PriorityHigh,
            TimeoutMs = 5000,
            Actor = "billing-job",
        });

        // Events: subscribe by type, publish from anywhere on the bus.
        var listener = new EventListener(context.Connection);
        await listener.InitAsync(null, "");
        await listener.SubscribeAsync<Calculated>((e, type, topic) =>
        {
            Console.WriteLine(e.Operation);
            return Task.CompletedTask;
        });
        await listener.StartAsync();
        await context.PublishEventAsync(new Calculated { Operation = "add", At = CustomTypes.Timestamp(DateTimeOffset.UtcNow) });
    }
}

// Streaming, server side: each yield is one chunk.
public class Assistant : AssistantProtobus.Base
{
    public Assistant(Context context) : base(context) { }

    public override async IAsyncEnumerable<Token> Generate(GenerateRequest request, CallContext context)
    {
        var i = 0;
        foreach (var word in new[] { "hello", "from", "csharp" })
        {
            if (context.CancellationToken.IsCancellationRequested) yield break; // the caller has gone
            await Task.Yield();
            yield return new Token { Index = i++, Text = word };
        }
    }

    // Client side: await foreach over the proxy's stream.
    public static async Task ReadAsync(AssistantProtobus.Proxy assistant)
    {
        await foreach (var token in assistant.Generate(new GenerateRequest { Prompt = "hi" }))
        {
            Console.Write(token.Text + " ");
            if (token.Index == 1) break; // leaving the loop tells the server to stop
        }
    }
}
```

---

## Concurrency

Deliveries are handled on the .NET thread pool, so handlers run in parallel
and must be safe for concurrent use. The number in flight is bounded by the
consumer's prefetch: `MaxConcurrent` for a service (default 1, one request at a
time), `DEFAULT_PREFETCH` for event handling (also 1). A streaming handler
holds its slot for the life of its stream. Replies are routed by a consumer of
their own, never behind a handler, so a handler that calls another service and
awaits its reply cannot starve the reply's delivery.

A `Context`, its proxies and its dispatchers are safe for concurrent use:
create one context per process and share it. See
[Architecture](docs/architecture.md).

---

## Wire compatibility

protobus-csharp speaks the protobus wire protocol exactly as TypeScript
protobus 2.5, protobus-py 2.0, protobus-go 2.0, protobus-cpp 2.0 and
protobus-java 2.0 do: the same exchanges, queues, envelopes (byte for byte),
headers and error codes, and the same environment variables for
configuration. Replicas of one service in different languages can share its
queue and climb one retry ladder together. The
[cross-language suite](crosslang/README.md) runs C# against the other ports'
real libraries over a real broker, in both directions.

The type mapping, the topology and the few deliberate behavioural differences
are in **[Compatibility](docs/compatibility.md)**.

---

## Documentation

Full index: **[docs/](docs/README.md)**

| Start | |
|---|---|
| [Getting Started](docs/getting-started.md) | From an empty directory to a service, a client, events and a test |
| [Services](docs/services.md) | Implementing services: options, retries, concurrency, lifecycle |
| [Clients](docs/clients.md) | Calling services: options, timeouts, errors, the untyped proxy |
| [Streaming](docs/streaming.md) | Server streaming and cancellation |
| [Events](docs/events.md) | Publishing and subscribing, topic patterns, event retry |
| [Errors](docs/errors.md) | The error model, retries and dead letters |

| Reference | |
|---|---|
| [Configuration](docs/configuration.md) | Environment variables, reconnection, logging |
| [Code generation](docs/codegen.md) | Protobus.Tools, the CLI, the protoc plugin, custom types |
| [Testing](docs/testing.md) | The in-memory broker and the suites |
| [Compatibility](docs/compatibility.md) | The wire contract and how the ports differ |
| [Architecture](docs/architecture.md) | Tasks, ownership and reconnection |
| [Security](docs/security.md) | Dispatch checks, error exposure, logging, TLS |

---

## Examples

| Example | Shows |
|---|---|
| [`Calculator`](examples/Calculator.cs) | A service, a client, a handled error and an event |
| [`Tokenstream`](examples/Tokenstream.cs) | Streaming tokens, cancelled three ways, with the server's own count |
| [`Combat`](examples/Combat.cs) | Six instances of one service playing a game over RPC and events |

```bash
docker compose up -d --wait   # RabbitMQ on 127.0.0.1:25672
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ dotnet run --project examples -- calculator
```

---

## License

MIT. See [LICENSE](LICENSE).
