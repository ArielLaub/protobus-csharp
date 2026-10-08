using System;
using System.Threading.Tasks;
using Pbtest;
using Xunit;

namespace Protobus.Tests;

public class LifecycleTests : MemoryBus
{
    [Fact]
    public async Task DrainWaitsForRunningHandlers()
    {
        var s = await ServeAsync(new MessageServiceOptions { MaxConcurrent = 4 });
        var call = Proxy().SlowAsync(new SlowRequest { Ms = 200 });
        Assert.True(await Eventually(() => s.SlowStarted == 1));
        await s.StopConsumingAsync();
        Assert.Equal(1, Ctx.Connection.InFlightDeliveries);
        Assert.True(await Ctx.Connection.DrainInFlightAsync(5000));
        // Stopping kept the channel open, so the reply still went out.
        await call.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task DrainReportsADeadlineItMissed()
    {
        var s = await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        _ = Proxy().SlowAsync(new SlowRequest { Ms = 2000 });
        Assert.True(await Eventually(() => s.SlowStarted == 1));
        Assert.False(await Ctx.Connection.DrainInFlightAsync(50));
    }

    private sealed class StubbornCalc : CalcService
    {
        public StubbornCalc(Context c, MessageServiceOptions? o) : base(c, o) { }

        public override async Task<Nothing> Slow(SlowRequest r, CallContext ctx)
        {
            await Task.Delay(r.Ms);
            return new Nothing();
        }
    }

    [Fact]
    public async Task AHandlerOutlivingItsTimeoutStillCountsAsRunning()
    {
        // A handler that ignores its token: the timeout settles its delivery, but it is still
        // running, and a drain must wait for it.
        await ServeAsync((c, o) => new StubbornCalc(c, o),
            new MessageServiceOptions { ProcessingTimeoutMs = 30, Retry = new RetryOptions(MaxRetries: 0) }, Ctx);
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().SlowAsync(new SlowRequest { Ms = 500 }));
        Assert.Equal("PROCESSING_TIMEOUT", e.Code);
        Assert.Equal(1, Ctx.Connection.InFlightDeliveries);
        Assert.False(await Ctx.Connection.DrainInFlightAsync(50));
        Assert.True(await Ctx.Connection.DrainInFlightAsync(5000));
        Assert.Equal(0, Ctx.Connection.InFlightDeliveries);
    }

    [Fact]
    public async Task HandlersRunInParallelUpToThePrefetch()
    {
        var s = await ServeAsync(new MessageServiceOptions { MaxConcurrent = 3 });
        var p = Proxy();
        for (var i = 0; i < 5; i++) _ = p.SlowAsync(new SlowRequest { Ms = 300 });
        Assert.True(await Eventually(() => s.SlowStarted == 3));
        await Task.Delay(100);
        Assert.Equal(3, s.SlowStarted);
    }

    [Fact]
    public async Task ClosingTheContextFailsPendingCalls()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        var client = await NewContextAsync();
        var p = Proxy(client);
        var call = p.SlowAsync(new SlowRequest { Ms = 3000 });
        await Task.Delay(50);
        await client.DisposeAsync();
        await Assert.ThrowsAsync<DisconnectedError>(() => call.WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<NotConnectedError>(() => p.AddAsync(Add(1, 1)));
    }

    [Fact]
    public async Task AServiceThatFailsToStartLeavesNothingConsuming()
    {
        // A retry queue declared with another TTL: the declare is refused.
        await (await ServeAsync(new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 1000) })).DisposeAsync();
        var other = await NewContextAsync();
        var s = new CalcService(other, new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 2000) });
        await Assert.ThrowsAsync<RetryQueueMismatchError>(s.InitAsync);
        Assert.Equal(0, Broker.ConsumerCount("pbtest.Calc"));
    }

    [Fact]
    public async Task AnUnknownServiceNameIsRefused()
    {
        var s = new CalcService.Instance(Ctx, null, "nope.Nothing");
        // Its own schema is registered, but no prefix of the name is a service.
        await Assert.ThrowsAsync<MissingProtoError>(s.InitAsync);
        Assert.Throws<InvalidServiceNameError>(() => new ServiceProxy(Ctx, "nope.Nothing").Init());
        var p = new ServiceProxy(Ctx, "pbtest.Calc");
        p.Init();
        Assert.Throws<AlreadyInitializedError>(p.Init);
    }

    [Fact]
    public async Task RunnableServiceShutsDownGracefully()
    {
        var c = await NewContextAsync();
        var s = await RunnableService.StartAsync(c, x => new CalcService(x));
        RunnableService.RequestShutdown();
        Assert.Equal(0, await RunnableService.WaitForShutdownAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(s.CleanedUp);
        Assert.False(c.IsConnected);
    }
}
