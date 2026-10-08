# Configuration

## Environment variables

The same variables, with the same defaults, as every other port. Each is read
when it is used, so a change at runtime is picked up. Integers must be all
digits and positive, or the default is kept; booleans are `1/true/yes/on` or
`0/false/no/off`. `Config.Set(name, value)` overrides one inside the process,
for tests and for applications that cannot set their environment;
`Config.Reset()` drops the overrides.

| Variable | Default | |
|---|---|---|
| `BUS_EXCHANGE_NAME` | `proto.bus` | RPC requests (topic) |
| `CALLBACKS_EXCHANGE_NAME` | `proto.bus.callback` | RPC replies (direct) |
| `EVENTS_EXCHANGE_NAME` | `proto.bus.events` | events (topic) |
| `CANCEL_EXCHANGE_NAME` | `proto.bus.cancel` | stream cancellation (fanout) |
| `MESSAGE_PROCESSING_TIMEOUT` | 600000 | one attempt at a unary request, ms |
| `RPC_CALL_TIMEOUT_MS` | 600000 | how long a caller waits, ms |
| `STREAM_IDLE_TIMEOUT_MS` | 60000 | the longest gap between stream chunks, ms |
| `STREAM_MAX_BUFFERED_CHUNKS` | 1024 | unconsumed chunks per stream |
| `STREAM_MAX_BUFFERED_BYTES` | 67108864 | unconsumed bytes per stream |
| `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 268435456 | unconsumed bytes across a context's streams |
| `DEFAULT_PREFETCH` | 1 | prefetch for late-ack consumers that set none (event listeners) |
| `PUBLISH_CONFIRM_TIMEOUT_MS` | 30000 | how long a publish waits for its confirm, ms |
| `MAX_OUTSTANDING_CONFIRMS` | 256 | publishes the broker has not answered, per channel (a timed-out one included); more wait for a slot |
| `CONNECTION_READY_TIMEOUT_MS` | 30000 | how long a publish waits through a reconnection, ms |
| `AMQP_HEARTBEAT_SECONDS` | 30 | heartbeat, unless the URL sets `heartbeat` |
| `SHUTDOWN_DRAIN_TIMEOUT_MS` | 30000 | how long shutdown waits for in-flight work, ms |
| `PROTOBUS_EXPOSE_INTERNAL_ERRORS` | true | send unhandled errors' messages to callers |
| `LOG_LEVEL` | info | debug, info, warn, error or silent |

The exchange names are part of the wire protocol: every process on one bus
must agree on them.

## The broker URL

`amqp://user:password@host:port/vhost`, or `amqps://` for TLS. A trailing `/`
is the default vhost, as in every port; `%2f` is too. Query parameters:
`heartbeat` (seconds, 0 disables), `connection_timeout` (ms), `channel_max`,
and `verify=verify_none` (see [Security](security.md#tls)).

## Context options

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Protobus;

public static class Setup
{
    public static async Task<Context> ContextAsync()
    {
        var context = new Context(new ContextOptions
        {
            // 10 attempts, 1 s doubling to 30 s, each with up to 30% jitter, by default.
            Reconnection = new ReconnectionOptions(MaxRetries: 0), // 0: forever
        });
        await context.InitAsync(Environment.GetEnvironmentVariable("AMQP_URL")!);
        return context;
    }
}
```

## Reconnection

When the connection drops, pending calls and streams fail with
`DisconnectedError`, and reconnection starts after the backoff delay. Before
the connection reports itself ready again, every service, listener and
dispatcher re-opens its channel and re-declares its queues, bindings and
consumers, in the order they were created; if any of that fails, the attempt
counts as failed and is retried. Calls made meanwhile wait, up to
`CONNECTION_READY_TIMEOUT_MS`. After `MaxRetries` consecutive failures the
connection gives up: `Error` subscribers hear a `ReconnectionError`, and
publishes fail with `NotReadyError`.

`context.Connection` reports `IsConnected`, `IsReconnecting` and `IsReady`,
and raises `Reconnecting`, `Reconnected`, `Disconnected` and `Error`.

A channel that the broker closes while the connection stays up (a consumer
timeout, a refused acknowledgement) is rebuilt the same way, with backoff, and
so is a consumer the broker cancels.

## Logging

protobus logs to the console by default, info and debug to standard output and
warnings and errors to standard error. `LOG_LEVEL`, or `Logger.Level`,
sets the threshold; `Logger.Set(sink)` replaces the sink with your own
`ILogSink`, for example one that forwards to `Microsoft.Extensions.Logging`:

<!-- doc-check: compile -->
```csharp
using Protobus;

public sealed class ForwardingSink : ILogSink
{
    public void Debug(string message) { /* logger.LogDebug(message) */ }
    public void Info(string message) { }
    public void Warn(string message) { }
    public void Error(string message) { }

    public static void Install() => Logger.Set(new ForwardingSink());
}
```

protobus never logs message bodies. See [Security](security.md#logging).
