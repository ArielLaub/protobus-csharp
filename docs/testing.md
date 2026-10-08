# Testing

## Your services, without RabbitMQ

`Protobus.Testing.MemoryBroker` is an AMQP broker in memory. Give it to a
context as its transport and everything runs in-process:

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;
using Protobus.Testing;

public static class InMemory
{
    public static async Task RunAsync()
    {
        await using var broker = new MemoryBroker();
        await using var context = new Context(new ContextOptions { Transport = broker });
        await context.InitAsync("amqp://memory/");
        // ...InitAsync services as usual...
        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();

        // Inspect the broker.
        int dead = broker.QueueDepth("Calculator.Service.DLQ");
        // Inject faults.
        broker.KillConnections(); // a lost socket: reconnection runs
        broker.SetConfirmMode(MemoryBroker.ConfirmMode.Nack);
        await MemoryBroker.WaitForAsync(() => context.Connection.IsReady, TimeSpan.FromSeconds(5));
    }
}
```

It implements what protobus relies on: direct, topic and fanout exchanges and
the default exchange; durable, exclusive, auto-delete and server-named queues;
per-queue TTL with dead-lettering; priority queues; per-consumer prefetch with
acknowledgement, rejection and redelivery; publisher confirms with mandatory
returns; and the channel errors RabbitMQ raises for a missing exchange or queue
(404), a redeclaration with different arguments (406) and an exclusive queue
owned elsewhere (405). Deliveries run one at a time per channel, each awaited
before the next, as RabbitMQ.Client dispatches them.

| Fault injection | |
|---|---|
| `KillConnections()` | drop every connection: unacked deliveries requeue, exclusive queues go |
| `RefuseConnections(true)` | make connecting fail |
| `SetConfirmMode(Nack / Drop)` | refuse publishes, or never confirm them |
| `ReleaseHeldConfirms(outcome)` | deliver the confirms `Drop` held back |
| `CloseChannelsConsuming(queue, reason)` | close a channel, leaving the connection up |
| `CancelConsumers(queue)` | cancel consumers from the broker side |

| Inspection | |
|---|---|
| `QueueExists`, `ExchangeExists`, `QueueNames` | topology |
| `QueueDepth`, `UnackedCount`, `ConsumerCount` | queue state |
| `Peek(queue)` | the waiting messages, with their properties and headers |
| `Bindings(queue, exchange)`, `QueueArguments(queue)` | declarations |
| `FlushAsync()` | wait until every queued callback has run |

Timeouts are environment-driven; `Config.Set("RPC_CALL_TIMEOUT_MS", "500")`
shortens one for a test, and `Config.Reset()` undoes it. `Config` is
process-wide, so tests that change it should not run in parallel.

## This repository's suites

| | Needs | Run |
|---|---|---|
| unit (runtime, generator, doc snippets) | nothing | `dotnet test --project tests/Protobus.Tests` |
| integration | RabbitMQ | `dotnet test --project tests/Protobus.IntegrationTests` |
| cross-language | RabbitMQ and the other ports | `dotnet test --project crosslang/Protobus.CrossLang.Tests` |
| examples | RabbitMQ | `dotnet run --project examples -- calculator` and friends |

The broker suites never default to a broker. Point them at one:

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
```

The unit suites run every behaviour against the in-memory broker: golden
envelope bytes shared with the TypeScript, Go, C++ and Java ports, retries and
dead-lettering with their headers, streaming with cancellation, backpressure,
sequence gaps and duplicates, events and event retry, reconnection, channel
and consumer loss, publish confirms, and graceful shutdown. The integration
suite repeats the parts only a real broker can prove: TTL and dead-lettering,
returns, priorities, header encodings, consumer cancellation and a broker-side
connection close. The [cross-language suite](../crosslang/README.md) runs C#
against the other five ports' real libraries.

`PROTOBUS_TEST_LOG=1` turns the library's logging on in every suite.
