# Clients

A client calls a service through its generated proxy. Build one per service
(they are cheap and safe for concurrent use), call `Init()`, and call the rpcs.

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public static class Calls
{
    public static async Task RunAsync(Context context, CancellationToken cancellationToken)
    {
        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();
        var request = new AddRequest { A = 2, B = 3 };

        var sum = await calculator.AddAsync(request);
        // With options and a token: the token stops the wait (the request may still run).
        var again = await calculator.AddAsync(request, new CallOptions { TimeoutMs = 2000 }, cancellationToken);
        Console.WriteLine($"{sum.Result} {again.Result}");
    }
}
```

A call's continuation runs on the thread pool, never on the AMQP client's
threads, so awaiting code may block without stalling the connection.

## Call options

`CallOptions` is an immutable record; set what you need with an initializer
or `with`.

| | Default | |
|---|---|---|
| `Actor` | none | a free-text caller identity, carried in the envelope for tracing |
| `TimeoutMs` | `RPC_CALL_TIMEOUT_MS` (10 min) | how long the call may take, confirm included |
| `Priority` | none | AMQP priority 0-255; only a priority queue honours it |
| `MessageId` | a fresh UUID | the message's identity, for deduplication |
| `Rpc = false` | `true` | publish without waiting for a reply; completes once the broker confirms |

The deadline starts once the connection is ready to publish: a call made during
a reconnection first waits for it, up to `CONNECTION_READY_TIMEOUT_MS` (30 s).
A cancelled token ends the wait with `OperationCanceledException`; like a
timeout, it says nothing about whether the service ran the request.

## What a call can throw

| | Meaning | Retry? |
|---|---|---|
| `RemoteError` | the service answered with an error; `Code` and `Message` are what it sent | depends on the code |
| `RpcTimeoutError` | no reply within the timeout | ambiguous: the service may have run it |
| `UnroutableError` | no service is bound to the routing key | safe |
| `PublishNackedError` | the broker refused the request | safe |
| `PublishConfirmTimeoutError` | no broker confirm in time (or no confirm slot free on the channel, in which case it was not sent) | ambiguous |
| `ChannelClosedError` | the channel closed before the confirm | ambiguous |
| `DisconnectedError` | the connection dropped while the call was pending | ambiguous |
| `NotReadyError` | the connection did not come back within `CONNECTION_READY_TIMEOUT_MS` | safe |
| `NotConnectedError` | the context is not connected and is not reconnecting | safe |
| `InvalidRequestError` | the request could not be encoded (a custom-type value out of range, say) | no |

"Ambiguous" means the request may have been stored and served. Retrying can
run it twice, so make the retry recognisable: pass the same `MessageId`,
derived from the work (an order id), and have the service deduplicate on
`context.MessageId`. Every port carries the id unchanged across redeliveries,
retries and dead-lettering.

All of these extend `ProtobusException`; the publish failures extend
`PublishError`, which carries the `MessageId`.

## Instance names

A proxy built with an instance name calls that instance, while the envelope
names the contract method:

```csharp
var player6 = new PlayerProtobus.Proxy(context, "Combat.Player.player6");
```

## The untyped proxy

`ServiceProxy` calls any service whose schema the context's factory knows, by
method name, typed with a message's parser or over serialized payloads:

<!-- doc-check: compile -->
```csharp
using System.Threading.Tasks;
using Calculator;
using Google.Protobuf;
using Protobus;

public static class Untyped
{
    public static async Task RunAsync(Context context)
    {
        context.Factory.Register(CalculatorReflection.Descriptor);
        var calculator = new ServiceProxy(context, "Calculator.Service");
        calculator.Init();
        AddResponse sum = await calculator.CallAsync("add", new AddRequest { A = 2, B = 3 }, AddResponse.Parser);
        byte[] raw = await calculator.CallRawAsync("add", new AddRequest { A = 4, B = 5 }.ToByteArray());
        await foreach (var chunk in calculator.CallStreamRaw("add", raw)) { } // InvalidRequestError: add is unary
    }
}
```

The proxy checks the method exists and is unary or streaming as called before
anything is sent. Google.Protobuf for C# has no dynamic messages, so there is
no descriptor-built message form.
