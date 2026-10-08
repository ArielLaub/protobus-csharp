using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Pbtest;
using Xunit;

namespace Protobus.Tests;

/// <summary>Regressions for the findings of the C# port's first audit.</summary>
public class AuditRegressionTests : MemoryBus
{
    [Fact]
    public async Task ADisposedTimerNeverFires()
    {
        var fired = 0;
        Ctx.Connection.Schedule(TimeSpan.FromMilliseconds(50), () => Interlocked.Increment(ref fired)).Dispose();
        await Task.Delay(250);
        Assert.Equal(0, fired);
    }

    [Fact]
    public async Task ATimerWhoseHandleIsDroppedStillFires()
    {
        var fired = 0;
        _ = Ctx.Connection.Schedule(TimeSpan.FromMilliseconds(100), () => Interlocked.Increment(ref fired));
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.True(await Eventually(() => fired == 1));
    }

    [Fact]
    public async Task AHealthyStreamOutlastingItsIdleWindowIsNotTimedOut()
    {
        var s = await ServeAsync(new MessageServiceOptions { MaxConcurrent = 2 });
        // 20 chunks 50 ms apart: about a second in all, never more than 50 ms between two.
        var seqs = new List<int>();
        await foreach (var t in Proxy().Ticks(new TickRequest { Count = 20, DelayMs = 50 }, new StreamOptions { IdleTimeoutMs = 300 }))
            seqs.Add(t.Seq);
        Assert.Equal(20, seqs.Count);
        Assert.True(s.Finished);
    }

    private sealed class BlockingCalc : CalcService
    {
        public BlockingCalc(Context c, MessageServiceOptions? o) : base(c, o) { }

        // Synchronous work before any await: the whole handler runs inside the call.
        public override Task<Nothing> Slow(SlowRequest r, CallContext ctx)
        {
            Interlocked.Increment(ref SlowStarted);
            Thread.Sleep(r.Ms);
            return Task.FromResult(new Nothing());
        }
    }

    [Fact]
    public async Task SynchronousHandlerWorkIsBoundedByTheProcessingTimeout()
    {
        await ServeAsync((c, o) => new BlockingCalc(c, o),
            new MessageServiceOptions { ProcessingTimeoutMs = 50, Retry = new RetryOptions(MaxRetries: 0) }, Ctx);
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().SlowAsync(new SlowRequest { Ms = 300 }));
        Assert.Equal("PROCESSING_TIMEOUT", e.Code);
        // Still running when the attempt was failed, and counted until it returns.
        Assert.True(await Ctx.Connection.DrainInFlightAsync(5000));
        Assert.Equal(0, Ctx.Connection.InFlightDeliveries);
    }

    [Fact]
    public async Task AHandlerThatNeverReturnsStillTimesOut()
    {
        using var release = new ManualResetEventSlim();
        await ServeAsync((c, o) => new HangingCalc(c, o, release),
            new MessageServiceOptions { ProcessingTimeoutMs = 50, Retry = new RetryOptions(MaxRetries: 0) }, Ctx);
        try
        {
            var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().SlowAsync(new SlowRequest()).WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal("PROCESSING_TIMEOUT", e.Code);
        }
        finally
        {
            release.Set();
        }
    }

    private sealed class HangingCalc : CalcService
    {
        private readonly ManualResetEventSlim release;

        public HangingCalc(Context c, MessageServiceOptions? o, ManualResetEventSlim release) : base(c, o) => this.release = release;

        public override Task<Nothing> Slow(SlowRequest r, CallContext ctx)
        {
            release.Wait();
            return Task.FromResult(new Nothing());
        }
    }
}

/// <summary>Regressions for the findings of the C# port's second audit.</summary>
public class SecondAuditRegressionTests : MemoryBus
{
    private static readonly Dictionary<string, object?> NotFinal = new() { [Config.HeaderFinal] = false };

    [Fact]
    public async Task AnIdleCallbackAlreadyRunningCannotExpireAStreamWhoseDeadlineWasReset()
    {
        var call = new MessageDispatcher.StreamCall(Ctx.MessageDispatcher, "s-1", 400);
        call.ArmIdle();
        lock (call.SyncRoot)
        {
            // The deadline passes while the lock is held: its callback starts and waits for it.
            Thread.Sleep(600);
            // A chunk arrives under the same lock and resets the deadline to 400 ms from now. A
            // stale callback runs the moment the lock is free, well inside that.
            call.OnChunk(new byte[] { 1 }, NotFinal);
        }
        await Task.Delay(50);
        Assert.False(call.Ended, "the stale callback expired a stream that had just made progress");
        Assert.NotNull(await call.NextAsync());
    }

    [Fact]
    public async Task ReadingAChunkResetsTheDeadlineWithNoGap()
    {
        var call = new MessageDispatcher.StreamCall(Ctx.MessageDispatcher, "s-2", 400);
        call.ArmIdle();
        call.OnChunk(new byte[] { 1 }, NotFinal);
        lock (call.SyncRoot)
        {
            Thread.Sleep(600);
            // Taking the chunk is progress too: the deadline it resets must be the one that counts.
            // A buffered chunk is taken synchronously, so this read never blocks.
            var read = call.NextAsync();
            Assert.True(read.IsCompletedSuccessfully);
#pragma warning disable xUnit1031
            Assert.NotNull(read.Result);
#pragma warning restore xUnit1031
        }
        await Task.Delay(50);
        Assert.False(call.Ended);
        call.Cancel(false);
    }
}
