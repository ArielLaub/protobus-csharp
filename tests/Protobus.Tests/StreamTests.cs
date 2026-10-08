using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Pbtest;
using Protobus.Amqp;
using Protobus.Internal;
using Xunit;

namespace Protobus.Tests;

public class StreamTests : MemoryBus
{
    private static TickRequest Ticks(int count) => new() { Count = count };

    private static async Task<List<int>> Seqs(IAsyncEnumerable<Tick> stream)
    {
        var o = new List<int>();
        await foreach (var t in stream) o.Add(t.Seq);
        return o;
    }

    [Fact]
    public async Task ChunksArriveInOrder()
    {
        var s = await ServeAsync();
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, await Seqs(Proxy().Ticks(Ticks(5))));
        Assert.True(s.Finished);
    }

    [Fact]
    public async Task AnEmptyStreamEnds()
    {
        await ServeAsync();
        Assert.Empty(await Seqs(Proxy().Ticks(Ticks(0))));
    }

    [Fact]
    public async Task AMidStreamHandledErrorEndsTheIteration()
    {
        await ServeAsync();
        var got = new List<int>();
        var e = await Assert.ThrowsAsync<RemoteError>(async () =>
        {
            await foreach (var t in Proxy().Ticks(new TickRequest { Count = 5, FailAt = 2 })) got.Add(t.Seq);
        });
        Assert.Equal(new[] { 0, 1 }, got);
        Assert.Equal("TEST_FAIL", e.Code);
    }

    [Fact]
    public async Task AMidStreamUnhandledErrorIsNotRetried()
    {
        var s = await ServeAsync();
        var e = await Assert.ThrowsAsync<RemoteError>(() =>
            Seqs(Proxy().Ticks(new TickRequest { Count = 5, FailAt = 1, Unhandled = true })));
        Assert.Equal("stream broke", e.Message);
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.DLQ"));
        Assert.Equal(1, s.Yielded);
    }

    [Fact]
    public async Task LeavingTheLoopEarlyCancelsTheProducer()
    {
        var s = await ServeAsync();
        await foreach (var t in Proxy().Ticks(new TickRequest { Count = 1000, DelayMs = 20 }))
        {
            Assert.Equal(0, t.Seq);
            break;
        }
        Assert.True(await Eventually(() => s.StoppedEarly));
        Assert.False(s.Finished);
        // Settled, not retried or dead-lettered.
        Assert.True(await Eventually(() => Broker.UnackedCount("pbtest.Calc") == 0));
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.DLQ"));
    }

    [Fact]
    public async Task ATokenCancelsFromAnywhere()
    {
        var s = await ServeAsync();
        using var cts = new CancellationTokenSource();
        await using var e = Proxy().Ticks(new TickRequest { Count = 1000, DelayMs = 20 }, cancellationToken: cts.Token)
            .GetAsyncEnumerator();
        Assert.True(await e.MoveNextAsync());
        cts.Cancel();
        Assert.True(await Eventually(() => s.StoppedEarly));
        // A cancelled stream ends rather than raising.
        var rest = 0;
        while (await e.MoveNextAsync()) rest++;
        Assert.True(rest <= 2, "at most what was already buffered");
    }

    [Fact]
    public async Task ACancelledTokenSendsNothing()
    {
        var s = await ServeAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Empty(await Seqs(Proxy().Ticks(Ticks(3), cancellationToken: cts.Token)));
        await Broker.FlushAsync();
        Assert.Equal(0, s.Yielded);
    }

    [Fact]
    public async Task AStalledStreamTimesOutAndStopsTheProducer()
    {
        var s = await ServeAsync();
        await Assert.ThrowsAsync<StreamTimeoutError>(() =>
            Seqs(Proxy().Ticks(new TickRequest { Count = 10, DelayMs = 2000 }, new StreamOptions { IdleTimeoutMs = 150 })));
        Assert.True(await Eventually(() => s.StoppedEarly));
    }

    [Fact]
    public async Task AProducerOutrunningItsConsumerFailsTheStream()
    {
        Config.Set("STREAM_MAX_BUFFERED_CHUNKS", "3");
        var s = await ServeAsync();
        await using var e = Proxy().Ticks(Ticks(50)).GetAsyncEnumerator();
        // Nothing is published until the first read; after it, the producer runs ahead of a
        // consumer that is not reading: past the three-chunk bound, the caller fails the stream
        // and the producer is told to stop.
        Assert.True(await e.MoveNextAsync());
        Assert.True(await Eventually(() => s.Yielded >= 5 || s.StoppedEarly));
        await Assert.ThrowsAsync<StreamBackpressureError>(async () =>
        {
            while (await e.MoveNextAsync()) { }
        });
    }

    /// <summary>A hand-driven "service" that replies with chunks of its own choosing.</summary>
    private async Task ReplyWithChunks(params Dictionary<string, object?>[] headers)
    {
        var c = Ctx.Connection;
        var ch = await c.OpenChannelAsync();
        await c.DeclareQueueAsync(ch, "pbtest.Calc", true, false, false);
        await ch.BindQueueAsync("pbtest.Calc", Config.BusExchangeName, "REQUEST.pbtest.Calc.*");
        await ch.ConsumeAsync("pbtest.Calc", "fake", true, false, d =>
        {
            _ = Task.Run(async () =>
            {
                for (var i = 0; i < headers.Length; i++)
                {
                    var body = MessageFactory.BuildEncodedResponse("pbtest.Calc.ticks", new Tick { Seq = i }.ToByteArray());
                    await c.PublishAsync(ch, Config.CallbacksExchangeName, d.Properties.ReplyTo!, body,
                        new PublishOptions(new MessageProperties { CorrelationId = d.Properties.CorrelationId, Headers = headers[i] }));
                }
            });
            return Task.CompletedTask;
        }, null);
    }

    [Fact]
    public async Task ALostChunkFailsTheStreamRatherThanTruncatingIt()
    {
        await ReplyWithChunks(
            new() { [Config.HeaderSeq] = 0, [Config.HeaderFinal] = false },
            new() { [Config.HeaderSeq] = 2, [Config.HeaderFinal] = true });
        await using var e = Proxy().Ticks(Ticks(3)).GetAsyncEnumerator();
        Assert.True(await e.MoveNextAsync());
        await Assert.ThrowsAsync<StreamSequenceError>(async () => await e.MoveNextAsync());
    }

    [Fact]
    public async Task PeersWithoutSequenceHeadersAndOtherEncodingsAreAccepted()
    {
        // A final flag as text, and no sequence numbers at all.
        await ReplyWithChunks(
            new() { [Config.HeaderFinal] = "false" },
            new() { [Config.HeaderFinal] = 0 },
            new() { [Config.HeaderFinal] = "true" });
        Assert.Equal(new[] { 0, 1, 2 }, await Seqs(Proxy().Ticks(Ticks(3))));
    }

    [Fact]
    public async Task ADuplicateChunkIsDropped()
    {
        await ReplyWithChunks(
            new() { [Config.HeaderSeq] = 0L, [Config.HeaderFinal] = false },
            new() { [Config.HeaderSeq] = 0L, [Config.HeaderFinal] = false },
            new() { [Config.HeaderSeq] = 1L, [Config.HeaderFinal] = true });
        // The second copy of seq 0 carried payload seq=1 but was dropped as a duplicate.
        Assert.Equal(new[] { 0, 2 }, await Seqs(Proxy().Ticks(Ticks(3))));
    }

    [Fact]
    public async Task AStreamRequestAgainstAUnaryMethodIsRefused()
    {
        await ServeAsync();
        var p = new ServiceProxy(Ctx, "pbtest.Calc");
        Ctx.Factory.Register(PbtestReflection.Descriptor);
        p.Init();
        await Assert.ThrowsAsync<InvalidRequestError>(() => Seqs(p.CallStream("add", Add(1, 2), Tick.Parser)));
    }

    [Fact]
    public async Task ChunksAreResponseContainers()
    {
        await ServeAsync();
        var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.ticks", null, Ticks(2).ToByteArray()));
        await using var raw = Ctx.PublishStreamingMessage(request, "REQUEST.pbtest.Calc.ticks");
        var chunks = new List<byte[]>();
        await foreach (var c in raw) chunks.Add(c);
        Assert.Equal(2, chunks.Count);
        Assert.Equal("pbtest.Calc.ticks", Envelopes.DecodeResponse(chunks[1]).Result!.Method);
    }
}
