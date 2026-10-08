using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Pbtest;

namespace Protobus.Tests;

/// <summary>Implements every rpc of pbtest.Calc except <c>unimplemented</c>, recording what it saw.</summary>
public class CalcService : CalcProtobus.Base
{
    public int FailAttempts;
    public int SlowStarted;
    public int SlowAborted;
    public int Yielded;
    public volatile bool StoppedEarly;
    public volatile bool Finished;
    public volatile bool CleanedUp;
    public readonly ConcurrentQueue<string?> FailMessageIds = new();

    public CalcService(Context context, MessageServiceOptions? options = null) : base(context, options) { }

    public override Task<AddResponse> Add(AddRequest r, CallContext ctx) => Task.FromResult(new AddResponse { Result = r.A + r.B });

    public override Task<DivideResponse> Divide(DivideRequest r, CallContext ctx)
    {
        if (r.Divisor == 0) throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        return Task.FromResult(new DivideResponse { Quotient = r.Dividend / r.Divisor });
    }

    public override async Task<Nothing> Fail(FailRequest r, CallContext ctx)
    {
        Interlocked.Increment(ref FailAttempts);
        FailMessageIds.Enqueue(ctx.MessageId);
        await Task.Yield();
        if (r.Handled) throw new HandledError("refused " + r.Id, "REFUSED");
        throw new InvalidOperationException("boom " + r.Id);
    }

    public override async Task<Nothing> Slow(SlowRequest r, CallContext ctx)
    {
        Interlocked.Increment(ref SlowStarted);
        try
        {
            await Task.Delay(r.Ms, ctx.CancellationToken);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Increment(ref SlowAborted);
        }
        return new Nothing();
    }

    public override async IAsyncEnumerable<Tick> Ticks(TickRequest r, CallContext ctx)
    {
        Yielded = 0;
        StoppedEarly = false;
        Finished = false;
        for (var i = 0; i < r.Count; i++)
        {
            if (r.FailAt > 0 && i >= r.FailAt)
            {
                if (r.Unhandled) throw new InvalidOperationException("stream broke");
                throw new HandledError("deliberate failure at chunk " + i, "TEST_FAIL");
            }
            if (ctx.CancellationToken.IsCancellationRequested)
            {
                StoppedEarly = true;
                yield break;
            }
            yield return new Tick { Seq = i, Payload = "chunk-" + i };
            Interlocked.Increment(ref Yielded);
            if (r.DelayMs > 0)
            {
                var cancelled = false;
                try
                {
                    await Task.Delay(r.DelayMs, ctx.CancellationToken);
                }
                catch (OperationCanceledException)
                {
                    cancelled = true;
                }
                if (cancelled)
                {
                    StoppedEarly = true;
                    yield break;
                }
            }
        }
        Finished = true;
    }

    public override Task<Who> Whoami(Nothing r, CallContext ctx) => Task.FromResult(new Who
    {
        Actor = ctx.Actor,
        MessageId = ctx.MessageId ?? "",
        RoutingKey = ctx.RoutingKey,
        Redelivered = ctx.Redelivered,
    });

    public override Task<Wallet> Echo(Wallet w, CallContext ctx) => Task.FromResult(w);

    protected override Task CleanupAsync()
    {
        CleanedUp = true;
        return Task.CompletedTask;
    }

    /// <summary>The same service under an instance name.</summary>
    public sealed class Instance : CalcService
    {
        private readonly string name;

        public Instance(Context context, MessageServiceOptions? options, string name) : base(context, options) => this.name = name;

        public override string ServiceName => name;
    }
}
