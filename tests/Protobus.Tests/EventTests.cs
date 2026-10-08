using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Pbtest;
using Protobus.Amqp;
using Xunit;

namespace Protobus.Tests;

public class EventTests : MemoryBus
{
    private static Ping Ping(string id) => new() { Id = id, N = CustomTypes.Bigint(7) };

    [Fact]
    public async Task ATypedSubscriberReceivesEventsOnTheDefaultTopic()
    {
        var s = await ServeAsync();
        var got = new ConcurrentQueue<string>();
        await s.SubscribeEventAsync<Ping>((e, type, topic) =>
        {
            got.Enqueue($"{e.Id} {type}@{topic}");
            return Task.CompletedTask;
        });
        await Ctx.PublishEventAsync(Ping("a"));
        Assert.True(await Eventually(() => got.Count == 1));
        Assert.Equal("a pbtest.Ping@EVENT.pbtest.Ping", got.First());
    }

    [Fact]
    public async Task EventsFanOutToEverySubscribingService()
    {
        var other = await NewContextAsync();
        var a = await ServeAsync();
        var b = await ServeAsync((c, o) => new CalcService.Instance(c, o, "pbtest.Calc.two"), null, other);
        var count = 0;
        await a.SubscribeEventAsync<Ping>((_, _, _) => Task.FromResult(Interlocked.Increment(ref count)));
        await b.SubscribeEventAsync<Ping>((_, _, _) => Task.FromResult(Interlocked.Increment(ref count)));
        await Ctx.PublishEventAsync(Ping("x"));
        Assert.True(await Eventually(() => count == 2));
    }

    [Fact]
    public async Task PatternsAndSubscribeAllMatchLikeTheBroker()
    {
        var s = await ServeAsync();
        var star = new ConcurrentQueue<string>();
        var all = new ConcurrentQueue<string>();
        await s.SubscribeEventAsync<Ping>((_, _, p) =>
        {
            star.Enqueue(p);
            return Task.CompletedTask;
        }, "billing.*");
        await s.SubscribeEventAsync<Pong>((_, t, _) =>
        {
            all.Enqueue(t);
            return Task.CompletedTask;
        }, "#");
        await Ctx.PublishEventAsync(Ping("1"), "billing.paid");
        await Ctx.PublishEventAsync(Ping("2"), "billing.paid.late");
        await Ctx.PublishEventAsync(new Pong { Id = "3" }, "other");
        await Ctx.PublishEventAsync(new Pong { Id = "4" }, "billing.paid");
        Assert.True(await Eventually(() => star.Count == 1 && all.Count == 2));
        await Broker.FlushAsync();
        Assert.Equal("billing.paid", star.Single());
    }

    [Fact]
    public async Task AHandlerOfAnotherTypeIsSkippedNotFed()
    {
        var s = await ServeAsync();
        var got = new ConcurrentQueue<IMessage>();
        await s.SubscribeEventAsync<Ping>((e, _, _) =>
        {
            got.Enqueue(e);
            return Task.CompletedTask;
        }, "shared");
        await s.SubscribeEventAsync<Pong>((e, _, _) =>
        {
            got.Enqueue(e);
            return Task.CompletedTask;
        }, "shared");
        await Ctx.PublishEventAsync(new Pong { Id = "p" }, "shared");
        Assert.True(await Eventually(() => got.Count == 1));
        await Broker.FlushAsync();
        Assert.Equal("pbtest.Pong", got.Single().Descriptor.FullName);
    }

    [Fact]
    public async Task AFailingHandlerDropsItsEventByDefault()
    {
        var s = await ServeAsync();
        var attempts = 0;
        await s.SubscribeEventAsync<Ping>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("nope");
        });
        await Ctx.PublishEventAsync(Ping("d"));
        Assert.True(await Eventually(() => attempts == 1));
        Assert.True(await Eventually(() => Broker.UnackedCount("pbtest.Calc.Events") == 0));
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.Events"));
        Assert.False(Broker.QueueExists("pbtest.Calc.Events.DLQ"));
    }

    [Fact]
    public async Task EventRetryClimbsItsOwnLadderToItsOwnDlq()
    {
        var s = await ServeAsync(new MessageServiceOptions { EventRetry = new EventRetryOptions(2, 10) });
        var attempts = 0;
        await s.SubscribeEventAsync<Ping>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("nope");
        });
        Assert.NotNull(Broker.QueueArguments("pbtest.Calc.Events.Retry"));
        await Ctx.PublishEventAsync(Ping("r"));
        Assert.True(await Eventually(() => Broker.QueueDepth("pbtest.Calc.Events.DLQ") == 1));
        Assert.Equal(3, attempts);
        Assert.Equal("EVENT.pbtest.Ping", Header(Broker.Peek("pbtest.Calc.Events.DLQ")[0], "x-original-routing-key"));
    }

    [Fact]
    public async Task AHandledErrorFromAnEventHandlerIsDroppedEvenWithRetryOn()
    {
        var s = await ServeAsync(new MessageServiceOptions { EventRetry = new EventRetryOptions(2, 10) });
        var attempts = 0;
        await s.SubscribeEventAsync<Ping>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new HandledError("not for me");
        });
        await Ctx.PublishEventAsync(Ping("h"));
        Assert.True(await Eventually(() => attempts == 1));
        Assert.True(await Eventually(() => Broker.UnackedCount("pbtest.Calc.Events") == 0));
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.Events.DLQ"));
    }

    [Fact]
    public async Task AnEventIsRoutedByTheKeyItArrivedOnNotItsBody()
    {
        var s = await ServeAsync();
        var got = new ConcurrentQueue<string>();
        await s.SubscribeEventAsync<Ping>((_, _, _) =>
        {
            got.Enqueue("secret");
            return Task.CompletedTask;
        }, "admin.only");
        await s.SubscribeEventAsync<Ping>((_, _, _) =>
        {
            got.Enqueue("public");
            return Task.CompletedTask;
        }, "public");
        // The body claims admin.only; the broker delivered it on "public".
        var forged = MessageFactory.BuildEvent("pbtest.Ping", Ping("f"), "admin.only");
        await Ctx.Connection.PublishAsync(await Ctx.Connection.OpenChannelAsync(), Config.EventsExchangeName, "public", forged,
            new PublishOptions(new MessageProperties()));
        Assert.True(await Eventually(() => got.Count == 1));
        await Broker.FlushAsync();
        Assert.Equal(new[] { "public" }, got);
    }

    [Fact]
    public async Task RetryOffDeclaresNoRetryTopology()
    {
        await ServeAsync();
        Assert.Null(Broker.QueueArguments("pbtest.Calc.Events.Retry"));
    }
}
