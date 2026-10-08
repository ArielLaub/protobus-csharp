# Architecture

The classes follow the TypeScript reference one to one.

| Class | Role |
|---|---|
| `Context` | one process's place on the bus: owns the connection, the factory and the two dispatchers |
| `Connection` | the AMQP connection: reconnection, restoration, confirmed publishing, the consume and settle loop |
| `MessageFactory` | schemas, and the request, reply and event envelopes |
| `MessageDispatcher` | the caller's side of RPC: publishes requests, routes replies and stream chunks |
| `EventDispatcher` | publishes events |
| `BaseListener` | a queue and its consumer, restored after a reconnection |
| `MessageListener` | a service's request queue, with its retry queue, retry exchange and DLQ |
| `EventListener` | an event queue and its topic router |
| `CallbackListener` | the context's reply queue |
| `CancelListener` | hears stream cancellations |
| `MessageService`, `RunnableService`, `ProxiedService` | the service base classes |
| `ServiceProxy` | calls a service by name |

`Amqp.ITransport` is the seam to the AMQP client: `RabbitTransport` over
RabbitMQ.Client 7 in production, `Testing.MemoryBroker` in tests.

## Tasks

Everything is asynchronous; nothing in the library blocks a thread.

| Where | Runs |
|---|---|
| RabbitMQ.Client's consumer dispatch | deliveries, one at a time per channel, each awaited; a service's delivery is handed on at once, a reply or a cancellation is routed right there |
| RabbitMQ.Client's reader loop | confirms, returns and closes |
| the thread pool | service and event handlers and their settlement, each channel's publish writer, reconnection and restoration, timers |

A delivery to a service is started on the thread pool at once, up to the
consumer's prefetch; the handler runs there and its delivery is settled there
(reply, ack, retry or dead-letter), each publish awaiting its broker confirm.
A delivery to the reply queue is routed in order by its consumer, which never
waits on a handler, so a handler awaiting a reply cannot starve the reply's
own delivery. Callers' tasks are completed asynchronously
(`RunContinuationsAsynchronously`), never inline on the client's threads.

## Settling a delivery

A late-ack consumer (every service, by default) settles after its handler:

1. On success, publish the reply (or the stream's chunks), then ack. The reply
   goes first, so the worst case is a redelivered request rather than a
   settled one whose reply was lost.
2. On a `HandledError` (or a protocol error, which is handled), reply with it,
   then ack.
3. On any other failure, publish the message to the retry exchange with
   `x-retry-count` incremented, then ack; or, after the last retry, reply with
   the error, publish it to the DLQ, then ack. The reply is best effort: the
   dead-letter copy is the durable record, and a failed reply never stops it.
4. If a settlement publish itself fails, the delivery is requeued after a
   second.

Retry and dead-letter copies are published `mandatory`, and the ack waits for
their confirm, so the only copy of a message is never acknowledged on the
strength of a publish that went nowhere.

## Reconnection

Every listener and dispatcher registers a restorer with the connection. When
the socket drops: the generation number advances, pending calls fail, and
reconnection is scheduled with backoff. After connecting, the restorers run in
registration order (each re-opens its channel and re-declares its topology),
and only when all have succeeded does the connection report itself ready and
release the publishes waiting on it. A restorer that fails, or a socket that
drops during restoration, fails the whole attempt; a stale attempt, whose
generation was superseded meanwhile, discards itself. RabbitMQ.Client's own
automatic recovery is turned off: protobus restores its topology itself, the
same way in every port.

## Publishing

Every channel is a confirm channel, and its publishes are written by one
writer of its own, in order: a socket write can wait on a full buffer or broker
flow control, and the caller (and its deadline) never wait on it. A publish
records its sequence number and waits for the broker's ack or nack; a mandatory
publish that comes back as a basic.return is reported `UnroutableError` when
its ack arrives. At most `MAX_OUTSTANDING_CONFIRMS` publishes await the
broker's answer per channel. A publish whose confirm timed out keeps its slot
until the broker answers or the channel closes, so a stalled broker cannot be
handed an unbounded backlog; a publish still waiting for a slot when its
deadline passes fails without being sent. Two pending mandatory publishes that
share a messageId carry an `x-protobus-publish-tag` header, so their returns
cannot be confused.
