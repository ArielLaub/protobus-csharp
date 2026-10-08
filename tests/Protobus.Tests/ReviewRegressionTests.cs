using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Pbtest;
using Protobus.Amqp;
using Protobus.Internal;
using Protobus.Testing;
using Xunit;

namespace Protobus.Tests;

/// <summary>A transport whose writes can be made to block, as a full socket would.</summary>
public sealed class StallingTransport : ITransport
{
    private readonly ITransport inner;
    public volatile TaskCompletionSource? Stall;
    /// <summary>Completed when a write reaches the stall, so a test knows it is blocked.</summary>
    public readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public StallingTransport(ITransport inner) => this.inner = inner;

    public async Task<IAmqpConnection> ConnectAsync(string url, int heartbeatSeconds) =>
        new Conn(this, await inner.ConnectAsync(url, heartbeatSeconds));

    private sealed class Conn : IAmqpConnection
    {
        private readonly StallingTransport t;
        private readonly IAmqpConnection c;

        internal Conn(StallingTransport t, IAmqpConnection c)
        {
            this.t = t;
            this.c = c;
        }

        public async Task<IAmqpChannel> OpenChannelAsync() => new Chan(t, await c.OpenChannelAsync());
        public Task CloseAsync() => c.CloseAsync();
        public bool IsOpen => c.IsOpen;
        public void OnClose(Action<string?> listener) => c.OnClose(listener);
    }

    private sealed class Chan : IAmqpChannel
    {
        private readonly StallingTransport t;
        private readonly IAmqpChannel ch;

        internal Chan(StallingTransport t, IAmqpChannel ch)
        {
            this.t = t;
            this.ch = ch;
        }

        public async Task PublishAsync(string exchange, string routingKey, byte[] body, MessageProperties properties,
            bool mandatory, Action<ConfirmOutcome, string> onConfirm)
        {
            if (t.Stall is { } s)
            {
                t.Entered.TrySetResult();
                await s.Task;
            }
            await ch.PublishAsync(exchange, routingKey, body, properties, mandatory, onConfirm);
        }

        public Task DeclareExchangeAsync(string n, string ty, bool d, bool a, bool i, IDictionary<string, object?> args) =>
            ch.DeclareExchangeAsync(n, ty, d, a, i, args);
        public Task<string> DeclareQueueAsync(string n, bool d, bool e, bool a, IDictionary<string, object?> args) =>
            ch.DeclareQueueAsync(n, d, e, a, args);
        public Task BindQueueAsync(string q, string x, string k) => ch.BindQueueAsync(q, x, k);
        public Task UnbindQueueAsync(string q, string x, string k) => ch.UnbindQueueAsync(q, x, k);
        public Task DeleteQueueAsync(string n) => ch.DeleteQueueAsync(n);
        public Task PurgeQueueAsync(string n) => ch.PurgeQueueAsync(n);
        public Task PrefetchAsync(ushort count) => ch.PrefetchAsync(count);
        public Task<string> ConsumeAsync(string q, string tag, bool n, bool e, Func<Delivery, Task> d, Action? c) =>
            ch.ConsumeAsync(q, tag, n, e, d, c);
        public Task CancelAsync(string tag) => ch.CancelAsync(tag);
        public Task AckAsync(ulong tag) => ch.AckAsync(tag);
        public Task RejectAsync(ulong tag, bool requeue) => ch.RejectAsync(tag, requeue);
        public Task CloseAsync() => ch.CloseAsync();
        public bool IsOpen => ch.IsOpen;
        public void OnClose(Action<string> listener) => ch.OnClose(listener);
    }
}

/// <summary>Regressions for the findings of the Java port's four reviews, where they apply to this one.</summary>
public class ReviewRegressionTests : MemoryBus
{
    private string ReplyQueue() => Broker.QueueNames.First(n =>
        n.StartsWith("amq.gen-") && Broker.Bindings(n, Config.CallbacksExchangeName).Count > 0);

    // ---- first review ------------------------------------------------------------------

    [Fact]
    public async Task ACallerBlockingInItsContinuationDoesNotStallReplies()
    {
        await ServeAsync();
        var p = Proxy();
        // A continuation that blocks, and would block reply delivery with it if it ran inline.
        var blocked = p.AddAsync(Add(1, 1)).ContinueWith(_ => Thread.Sleep(1500), TaskContinuationOptions.ExecuteSynchronously);
        await Task.Delay(50);
        var sw = Stopwatch.StartNew();
        Assert.Equal(4, (await p.AddAsync(Add(2, 2))).Result);
        Assert.True(sw.ElapsedMilliseconds < 1000, $"took {sw.ElapsedMilliseconds} ms");
        await blocked;
    }

    [Fact]
    public async Task AnEventPublisherBlockingInItsContinuationDoesNotStallConfirms()
    {
        var blocked = Ctx.PublishEventAsync(new Ping { Id = "t" })
            .ContinueWith(_ => Thread.Sleep(1500), TaskContinuationOptions.ExecuteSynchronously);
        await Task.Delay(50);
        var sw = Stopwatch.StartNew();
        await Ctx.PublishEventAsync(new Ping { Id = "u" });
        Assert.True(sw.ElapsedMilliseconds < 1000, $"took {sw.ElapsedMilliseconds} ms");
        await blocked;
    }

    [Fact]
    public async Task TheRpcDeadlineBoundsTheConfirm()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        var p = Proxy();
        // The confirm never comes and neither does a reply (the handler outlasts the deadline):
        // the deadline, not the 5 s confirm timeout, ends the call.
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAsync<RpcTimeoutError>(() => p.SlowAsync(new SlowRequest { Ms = 3000 }, new CallOptions { TimeoutMs = 150 }));
        Assert.True(sw.ElapsedMilliseconds < 2000);
    }

    private sealed class CountingCalc : CalcService
    {
        public int Running;
        public int Peak;

        public CountingCalc(Context c, MessageServiceOptions? o) : base(c, o) { }

        public override async Task<Nothing> Slow(SlowRequest r, CallContext ctx)
        {
            var now = Interlocked.Increment(ref Running);
            int seen;
            while ((seen = Peak) < now && Interlocked.CompareExchange(ref Peak, now, seen) != seen) { }
            try
            {
                return await base.Slow(r, ctx);
            }
            finally
            {
                Interlocked.Decrement(ref Running);
            }
        }
    }

    [Fact]
    public async Task AnEarlyAckServiceRunsAtMostMaxConcurrentHandlers()
    {
        var s = await ServeAsync((c, o) => new CountingCalc(c, o), new MessageServiceOptions { LateAck = false, MaxConcurrent = 2 }, Ctx);
        var p = Proxy();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => p.SlowAsync(new SlowRequest { Ms = 50 })))
            .WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(8, s.SlowStarted);
        Assert.True(s.Peak <= 2, "peak " + s.Peak);
    }

    [Fact]
    public async Task CallsPendingWhenTheReplyQueueIsReplacedFailAtOnce()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        var call = Proxy().SlowAsync(new SlowRequest { Ms = 3000 });
        Broker.CloseChannelsConsuming(ReplyQueue(), "406 PRECONDITION_FAILED - unknown delivery tag");
        await Assert.ThrowsAsync<DisconnectedError>(() => call.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task AFailedStreamReleasesItsEntryWithoutBeingRead()
    {
        await ServeAsync(new MessageServiceOptions { MaxConcurrent = 2, ProcessingTimeoutMs = 60000 });
        await using var stream = Proxy().Ticks(new TickRequest { Count = 100, DelayMs = 100 }).GetAsyncEnumerator();
        Assert.True(await stream.MoveNextAsync());
        Broker.KillConnections();
        Assert.True(await Eventually(() => Ctx.MessageDispatcher.PendingStreamCount == 0));
    }

    // ---- second review -----------------------------------------------------------------

    [Fact]
    public async Task ATimedOutConfirmKeepsItsSlotUntilTheBrokerAnswers()
    {
        Config.Set("MAX_OUTSTANDING_CONFIRMS", "1");
        Config.Set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
        var ch = await Ctx.Connection.OpenChannelAsync();
        await Ctx.Connection.DeclareQueueAsync(ch, "q", true, false, false);
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var plain = new PublishOptions(new MessageProperties());
        var publishes = Enumerable.Range(0, 4).Select(_ => Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), plain)).ToList();
        foreach (var f in publishes) await Assert.ThrowsAsync<PublishConfirmTimeoutError>(() => f.WaitAsync(TimeSpan.FromSeconds(5)));
        await Broker.FlushAsync();
        // The bound holds: the broker never had more than one unanswered publish.
        Assert.Equal(1, Broker.HeldConfirms);
        // Once the broker answers, the slot is free again.
        Broker.ReleaseHeldConfirms(ConfirmOutcome.Ack);
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Ack);
        await Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), plain).WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task AFastReplyDoesNotDisarmTheDeadline()
    {
        await ServeAsync();
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var sw = Stopwatch.StartNew();
        // The reply arrives at once (the broker routes it), but the request's confirm never
        // comes: the 100 ms deadline must still end the call.
        await Assert.ThrowsAsync<RpcTimeoutError>(() => Proxy().AddAsync(Add(1, 1), new CallOptions { TimeoutMs = 100 }));
        Assert.True(sw.ElapsedMilliseconds < 2000, $"took {sw.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task TheIdleTimeoutWakesAReaderWaitingOnThePublish()
    {
        await ServeAsync();
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var sw = Stopwatch.StartNew();
        await using var stream = Proxy().Ticks(new TickRequest { Count = 3, DelayMs = 2000 }, new StreamOptions { IdleTimeoutMs = 80 })
            .GetAsyncEnumerator();
        await Assert.ThrowsAsync<StreamTimeoutError>(async () => await stream.MoveNextAsync());
        Assert.True(sw.ElapsedMilliseconds < 2000, $"took {sw.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task ACompletedStreamKeepsItsChunksAcrossADisconnect()
    {
        await ServeAsync();
        var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.ticks", null,
            new TickRequest { Count = 2 }.ToByteArray()));
        await using var raw = Ctx.PublishStreamingMessage(request, "REQUEST.pbtest.Calc.ticks");
        Assert.True(await Eventually(() => raw.Ended));
        Broker.KillConnections();
        Assert.True(await Eventually(() => !Ctx.Connection.IsConnected || Ctx.IsReconnecting));
        var chunks = new List<byte[]>();
        await foreach (var c in raw) chunks.Add(c);
        Assert.Equal(2, chunks.Count);
        Assert.Equal(1, Tick.Parser.ParseFrom(Envelopes.DecodeResponse(chunks[1]).Result!.Data).Seq);
    }

    [Fact]
    public async Task ReadingAStreamAfterTheContextClosedRaisesItsOwnOutcome()
    {
        var c = await NewContextAsync();
        var s = await ServeAsync();
        await using var stream = Proxy(c).Ticks(new TickRequest { Count = 100, DelayMs = 100 }).GetAsyncEnumerator();
        // The request is published by the first read; let it start, then close underneath it.
        var first = stream.MoveNextAsync().AsTask();
        await c.DisposeAsync();
        await Assert.ThrowsAsync<DisconnectedError>(async () =>
        {
            if (await first) while (await stream.MoveNextAsync()) { }
        });
        Assert.Equal(0, s.FailAttempts);
    }

    [Fact]
    public void CollidingRpcNamesStillGenerateDistinctMethods()
    {
        // naming.proto compiles at all only if the names were allocated apart.
        var names = typeof(Naming.NamingProtobus.Proxy).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(m => m.Name).ToList();
        Assert.Contains("FetchAsyncAsyncAsync", names);
        // fetch's own proxy method could not be FetchAsync, which is the base name of an rpc.
        Assert.Contains("FetchAsync_", names);
        Assert.Contains("FetchAsyncAsync_", names);
        Assert.Contains("Watch", names);
        Assert.Equal(names.Count, names.Distinct().Count());
    }

    // ---- third review ------------------------------------------------------------------

    [Fact]
    public async Task ABlockedWriteDoesNotHoldTheCallerPastItsDeadline()
    {
        var transport = new StallingTransport(Broker);
        var c = new Context(new ContextOptions { Transport = transport });
        await c.InitAsync("amqp://memory/");
        await ServeAsync();
        transport.Stall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.add", null, Add(1, 1).ToByteArray()));
            var sw = Stopwatch.StartNew();
            var call = c.PublishMessageAsync(request, "REQUEST.pbtest.Calc.add", new CallOptions { TimeoutMs = 50 });
            Assert.True(sw.ElapsedMilliseconds < 200, $"PublishMessageAsync took {sw.ElapsedMilliseconds} ms to return");
            await Assert.ThrowsAsync<RpcTimeoutError>(() => call.WaitAsync(TimeSpan.FromSeconds(2)));
        }
        finally
        {
            transport.Stall.TrySetResult();
            await c.DisposeAsync();
        }
    }

    [Fact]
    public async Task ACancelledStreamStaysCancelledWhenItsPublishFailsLater()
    {
        Config.Set("PUBLISH_CONFIRM_TIMEOUT_MS", "100");
        await ServeAsync();
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        using var stop = new CancellationTokenSource();
        var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.ticks", null,
            new TickRequest { Count = 3, DelayMs = 1000 }.ToByteArray()));
        await using var stream = Ctx.PublishStreamingMessage(request, "REQUEST.pbtest.Calc.ticks", null, stop.Token);
        stop.Cancel();
        await Task.Delay(300); // past the request's confirm deadline
        Assert.Null(await stream.NextAsync());
    }

    // ---- fourth review -----------------------------------------------------------------

    /// <summary>
    /// The second publish waits behind a write blocked in the transport: queued for the writer
    /// when a confirm slot is free (limit 2), or for a slot when none is (limit 1). Either way,
    /// closing the context must fail it at once.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ClosingFailsPublishesQueuedBehindABlockedWrite(int limit)
    {
        Config.Set("MAX_OUTSTANDING_CONFIRMS", limit.ToString());
        Config.Set("PUBLISH_CONFIRM_TIMEOUT_MS", "300");
        var transport = new StallingTransport(Broker);
        var c = new Context(new ContextOptions { Transport = transport });
        await c.InitAsync("amqp://memory/");
        var ch = await c.Connection.OpenChannelAsync();
        await c.Connection.DeclareQueueAsync(ch, "q", true, false, false);
        var stall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Stall = stall;
        var plain = new PublishOptions(new MessageProperties());
        try
        {
            var first = c.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), plain);
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
            var second = c.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), plain);
            await c.DisposeAsync();
            await Assert.ThrowsAsync<ChannelClosedError>(() => second.WaitAsync(TimeSpan.FromMilliseconds(500)));
            Assert.True(!first.IsCompleted || first.IsFaulted);
            // A publish made after the close fails at once too.
            var late = c.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), plain);
            Assert.True(late.IsFaulted);
        }
        finally
        {
            stall.TrySetResult();
        }
    }
}
