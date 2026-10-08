using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Pbtest;
using Xunit;

namespace Protobus.Tests;

public class RecoveryTests : MemoryBus
{
    [Fact]
    public async Task ServicesAndClientsComeBackAfterAConnectionLoss()
    {
        var s = await ServeAsync();
        var p = Proxy();
        Assert.Equal(3, (await p.AddAsync(Add(1, 2))).Result);
        var reconnected = 0;
        Ctx.Connection.Reconnected += () => Interlocked.Increment(ref reconnected);
        Broker.KillConnections();
        Assert.True(await Eventually(() => reconnected == 1));
        Assert.True(Ctx.Connection.IsReady);
        Assert.Equal(7, (await p.AddAsync(Add(3, 4))).Result);
        Assert.Equal(1, Broker.ConsumerCount("pbtest.Calc"));
        // Events too: the event queue's consumer and bindings are back.
        var got = new ConcurrentQueue<string>();
        await s.SubscribeEventAsync<Ping>((e, _, _) =>
        {
            got.Enqueue(e.Id);
            return Task.CompletedTask;
        });
        await Ctx.PublishEventAsync(new Ping { Id = "after" });
        Assert.True(await Eventually(() => got.Count == 1));
    }

    [Fact]
    public async Task PendingCallsAndStreamsFailWhenTheConnectionDrops()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000, MaxConcurrent = 2 });
        var p = Proxy();
        var call = p.SlowAsync(new SlowRequest { Ms = 3000 });
        await using var stream = p.Ticks(new TickRequest { Count = 100, DelayMs = 200 }).GetAsyncEnumerator();
        Assert.True(await stream.MoveNextAsync());
        Broker.KillConnections();
        await Assert.ThrowsAsync<DisconnectedError>(() => call.WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<DisconnectedError>(async () =>
        {
            while (await stream.MoveNextAsync()) { }
        });
    }

    [Fact]
    public async Task ACallMadeDuringAReconnectionWaitsForIt()
    {
        await ServeAsync();
        var p = Proxy();
        Broker.RefuseConnections(true);
        Broker.KillConnections();
        Assert.True(await Eventually(() => Ctx.IsReconnecting));
        var call = p.AddAsync(Add(2, 2));
        await Task.Delay(50);
        Assert.False(call.IsCompleted);
        Broker.RefuseConnections(false);
        Assert.Equal(4, (await call.WaitAsync(TimeSpan.FromSeconds(5))).Result);
    }

    [Fact]
    public async Task AReconnectionThatNeverSucceedsGivesUp()
    {
        var c = await NewContextAsync(new ContextOptions { Reconnection = FastReconnect() with { MaxRetries = 3 } });
        var errors = new ConcurrentQueue<Exception>();
        c.Connection.Error += errors.Enqueue;
        Broker.RefuseConnections(true);
        Broker.KillConnections();
        Assert.True(await Eventually(() => errors.OfType<ReconnectionError>().Any()));
        var e = await Assert.ThrowsAsync<NotReadyError>(() => c.Connection.WhenReadyAsync(1000));
        Assert.Contains("max reconnection attempts", e.Message);
    }

    [Fact]
    public async Task AWaitOnReadinessIsBounded()
    {
        Broker.RefuseConnections(true);
        Broker.KillConnections();
        Assert.True(await Eventually(() => Ctx.IsReconnecting));
        await Assert.ThrowsAsync<NotReadyError>(() => Ctx.Connection.WhenReadyAsync(50));
    }

    [Fact]
    public async Task AChannelLostOnALiveConnectionIsRebuilt()
    {
        await ServeAsync();
        var p = Proxy();
        Broker.CloseChannelsConsuming("pbtest.Calc", "541 INTERNAL_ERROR");
        Assert.True(await Eventually(() => Broker.ConsumerCount("pbtest.Calc") == 1));
        Assert.Equal(9, (await p.AddAsync(Add(4, 5))).Result);
        Assert.True(Broker.OpenConnections > 0);
    }

    [Fact]
    public async Task AConsumerCancelledByTheBrokerIsRestored()
    {
        await ServeAsync();
        Broker.CancelConsumers("pbtest.Calc");
        Assert.True(await Eventually(() => Broker.ConsumerCount("pbtest.Calc") == 1));
        Assert.Equal(2, (await Proxy().AddAsync(Add(1, 1))).Result);
    }

    [Fact]
    public async Task TheReplyQueueIsRebuiltWhenItsChannelIsLost()
    {
        await ServeAsync();
        var p = Proxy();
        var replyQueue = Broker.QueueNames.First(n => n.StartsWith("amq.gen-") && Broker.Bindings(n, Config.CallbacksExchangeName).Count > 0);
        Broker.CloseChannelsConsuming(replyQueue, "406 PRECONDITION_FAILED - unknown delivery tag");
        var ok = false;
        for (var i = 0; i < 50 && !ok; i++)
        {
            try
            {
                ok = (await p.AddAsync(Add(1, 1), new CallOptions { TimeoutMs = 300 })).Result == 2;
            }
            catch (ProtobusException)
            {
                await Task.Delay(20);
            }
        }
        Assert.True(ok);
    }

    [Fact]
    public async Task AStoppedServiceIsNotRestoredByAReconnection()
    {
        var s = await ServeAsync();
        await s.StopConsumingAsync();
        var reconnected = 0;
        Ctx.Connection.Reconnected += () => Interlocked.Increment(ref reconnected);
        Broker.KillConnections();
        Assert.True(await Eventually(() => reconnected == 1));
        await Broker.FlushAsync();
        Assert.Equal(0, Broker.ConsumerCount("pbtest.Calc"));
    }
}
