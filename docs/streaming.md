# Streaming

A method declared `returns (stream T)` streams its reply: the service yields
chunks, the caller enumerates them.

```protobuf
service Assistant {
  rpc generate(GenerateRequest) returns (stream Token);
}
```

## The service

The generated base declares the rpc as an `IAsyncEnumerable<T>`: write it as
an async iterator. Each `yield return` is a chunk; returning ends the stream;
throwing ends it with the error, which the caller's iteration raises.

<!-- doc-check: compile -->
```csharp
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Chat;
using Protobus;

public class Assistant : AssistantProtobus.Base
{
    public Assistant(Context context) : base(context) { }

    public override async IAsyncEnumerable<Token> Generate(GenerateRequest request, CallContext context)
    {
        if (request.Prompt.Length == 0) throw new HandledError("empty prompt", "EMPTY_PROMPT");
        var words = request.Prompt.Split(' ');
        for (var i = 0; i < words.Length; i++)
        {
            // Fires when the caller cancels or stops listening: stop the work, not just the yielding.
            if (context.CancellationToken.IsCancellationRequested) yield break;
            await Task.Delay(50);
            yield return new Token { Index = i, Text = words[i] };
        }
    }
}
```

A chunk is held until the next one is yielded (or the iterator ends), so the
last chunk can be marked final: `x-protobus-final=true` on the last message,
`x-protobus-seq` numbering every one from 0. A stream that yields nothing still
sends one empty final message, so the caller's loop ends.

An error is never retried: the stream ends with a terminal error chunk, which
a `HandledError` fills with its own message and code, and anything else with
the same message a unary call would get (see [Errors](errors.md)). The
processing timeout does not apply to streams.

A streaming handler holds one of the service's `MaxConcurrent` slots for as
long as it runs.

## The caller

The proxy method returns an `IAsyncEnumerable<T>`; the request is published
when the enumeration starts, and a failure to publish surfaces from the first
`MoveNextAsync`.

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Chat;
using Protobus;

public static class Reader
{
    public static async Task ReadAsync(AssistantProtobus.Proxy assistant)
    {
        var request = new GenerateRequest { Prompt = "hello streaming world" };

        // Leaving the loop before the end cancels the call.
        await foreach (var token in assistant.Generate(request))
        {
            Console.Write(token.Text + " ");
            if (token.Index == 1) break;
        }

        // Or cancel from anywhere: a Stop button, a request's own deadline.
        using var stop = new CancellationTokenSource();
        await foreach (var token in assistant.Generate(request, new StreamOptions { IdleTimeoutMs = 5000 }, stop.Token))
            if (token.Index == 0) stop.Cancel(); // the loop ends; it does not throw
    }
}
```

| `StreamOptions` | Default | |
|---|---|---|
| `IdleTimeoutMs` | `STREAM_IDLE_TIMEOUT_MS` (60 s) | the longest gap allowed between chunks |
| `Actor` | none | the caller's identity, as for unary calls |

A cancelled stream ends rather than raising, as in every port; that is why
cancelling is not an `OperationCanceledException` here. `WithCancellation`
on the enumerable works the same way as the method's own token.

What the iteration can raise:

| | |
|---|---|
| `RemoteError` | the service's error, after the chunks before it |
| `StreamTimeoutError` | no chunk within the idle timeout; the producer is told to stop |
| `StreamBackpressureError` | the caller fell behind: more than `STREAM_MAX_BUFFERED_CHUNKS` or `STREAM_MAX_BUFFERED_BYTES` buffered for this call, or `STREAM_MAX_TOTAL_BUFFERED_BYTES` across the context |
| `StreamSequenceError` | a chunk was lost (a gap in `x-protobus-seq`) |
| `DisconnectedError` | the connection dropped |

A duplicate chunk (a broker redelivery) is dropped. A peer that sends no
sequence numbers is accepted as it is. A gap or a buffer overflow fails the
stream at once and drops what was buffered, as the TypeScript reference does.

## Cancellation

Cancelling (leaving the loop early, cancelling its token, or the idle timeout)
releases the call's buffer at once and publishes a notice on the
`proto.bus.cancel` fanout exchange. The replica running that stream fires the
handler's `CancellationToken` and stops publishing: the iterator is disposed
after its next `yield`, which runs its `finally` blocks. Cancellation is
cooperative and best effort: a handler that never looks at its token runs to
its next `yield`, and a lost notice is the same as none.
