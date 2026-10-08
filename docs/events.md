# Events

Events are fire-and-forget messages on the `proto.bus.events` topic exchange.
Every subscriber with a matching binding gets its own copy; a publisher does
not know or wait for subscribers.

## Publishing

```csharp
await context.PublishEventAsync(calculated);                  // topic from the type: EVENT.Calculator.Calculated
await context.PublishEventAsync(calculated, "billing.done");  // a topic of your own
await service.PublishEventAsync(calculated);                  // the same, from inside a service
```

`PublishEventAsync` completes once the broker has confirmed the event. An
event nobody subscribes to is dropped by the broker and is not an error.

## Subscribing

A service subscribes after `InitAsync()`, on its own `<Service>.Events` queue:

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public static class Subscriptions
{
    public static async Task SubscribeAsync(MessageService service)
    {
        // On the default topic EVENT.Calculator.Calculated.
        await service.SubscribeEventAsync<Calculated>((e, type, topic) =>
        {
            Console.WriteLine(e.Operation);
            return Task.CompletedTask;
        });
        // On a pattern: * is one word, # is zero or more.
        await service.SubscribeEventAsync<Calculated>((e, type, topic) =>
        {
            Console.WriteLine(topic);
            return Task.CompletedTask;
        }, "billing.*");
    }
}
```

Several handlers may match one event; each runs once. A handler only receives
events of its own type; one of another type on the same topic is skipped (with
a warning), never decoded as the wrong type. Handlers are matched by the
routing key the broker delivered on, not by the topic written in the event's
body, so a publisher cannot reach handlers its routing key does not.

Outside a service, an `EventListener` subscribes on a queue of its own; with
an empty queue name it gets an exclusive, server-named queue that disappears
with the connection:

<!-- doc-check: compile -->
```csharp
using System.Threading.Tasks;
using Calculator;
using Protobus;

public static class Standalone
{
    public static async Task<EventListener> ListenAsync(Context context)
    {
        var listener = new EventListener(context.Connection);
        await listener.InitAsync(null, "");
        await listener.SubscribeAsync<Calculated>((e, type, topic) => Task.CompletedTask);
        await listener.StartAsync();
        return listener; // CloseAsync() when done
    }
}
```

## When a handler fails

By default a handler that throws loses its event: the delivery is rejected
without requeue, so one permanently failing event cannot stall the subscriber
behind its own prefetch.

`MessageServiceOptions.EventRetry = new EventRetryOptions(MaxRetries: 3)` gives
events the ladder requests climb: the failed event waits on
`<Service>.Events.Retry` for `RetryDelayMs` (5 s by default), comes back to
this subscriber only (through `<Service>.Events.Redelivery`, never to the
others), and after the last retry lands on `<Service>.Events.DLQ`. A retried
event re-runs every handler that matched it, including the ones that
succeeded. A `HandledError` is not retried.
