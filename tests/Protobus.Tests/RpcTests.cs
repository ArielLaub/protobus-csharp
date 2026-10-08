using System;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Google.Protobuf;
using Pbtest;
using Protobus.Internal;
using Xunit;

namespace Protobus.Tests;

public class RpcTests : MemoryBus
{
    [Fact]
    public async Task UnaryCallsRoundTrip()
    {
        await ServeAsync();
        Assert.Equal(42, (await Proxy().AddAsync(Add(20, 22))).Result);
        Assert.Equal(0.25, (await Proxy().DivideAsync(new DivideRequest { Dividend = 1, Divisor = 4 })).Quotient);
    }

    [Fact]
    public async Task ConcurrentCallsComplete()
    {
        await ServeAsync();
        var p = Proxy();
        var results = await Task.WhenAll(p.AddAsync(Add(1, 1)), p.AddAsync(Add(2, 2)), p.AddAsync(Add(3, 3)));
        Assert.Equal(new[] { 2, 4, 6 }, results.Select(r => r.Result));
    }

    [Fact]
    public async Task AHandledErrorIsAnsweredAtOnceAndNeverRetried()
    {
        var s = await ServeAsync();
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().FailAsync(new FailRequest { Id = "7", Handled = true }));
        Assert.Equal("refused 7", e.Message);
        Assert.Equal("REFUSED", e.Code);
        Assert.Equal("pbtest.Calc.fail", e.Method);
        Assert.Equal(1, s.FailAttempts);
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.DLQ"));
    }

    [Fact]
    public async Task AnUnhandledErrorClimbsTheRetryLadderThenReachesTheDlq()
    {
        var s = await ServeAsync(new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 20) });
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().FailAsync(new FailRequest { Id = "9" }));
        // Exposed by default: the caller is another of your own services.
        Assert.Equal("boom 9", e.Message);
        Assert.Equal(4, s.FailAttempts);
        Assert.True(await Eventually(() => Broker.QueueDepth("pbtest.Calc.DLQ") == 1));
        var dead = Broker.Peek("pbtest.Calc.DLQ")[0];
        Assert.Equal("3", Header(dead, "x-retry-count"));
        Assert.Equal("REQUEST.pbtest.Calc.fail", Header(dead, "x-original-routing-key"));
        Assert.Equal("pbtest.Calc", Header(dead, "x-original-queue"));
        // Name only: an unhandled error's message never reaches a header.
        Assert.Equal("InvalidOperationException", Header(dead, "x-last-error"));
        Assert.NotEmpty(Header(dead, "x-first-failure-time"));
        Assert.NotEmpty(Header(dead, "x-dlq-time"));
        Assert.Equal("application/octet-stream", dead.Properties.ContentType);
        // One identity across every attempt and the dead-letter copy.
        Assert.Single(s.FailMessageIds.Distinct());
        Assert.Equal(s.FailMessageIds.First(), dead.Properties.MessageId);
    }

    [Fact]
    public async Task AnUnhandledErrorIsHiddenWhenExposureIsOff()
    {
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false");
        await ServeAsync(new MessageServiceOptions { Retry = new RetryOptions(MaxRetries: 0) });
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().FailAsync(new FailRequest { Id = "x" }));
        Assert.Equal("INTERNAL_ERROR", e.Code);
        Assert.StartsWith("internal service error (correlationId ", e.Message);
    }

    [Fact]
    public async Task AnUnimplementedRpcIsAProtocolError()
    {
        await ServeAsync();
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().UnimplementedAsync(new Nothing()));
        Assert.Equal("PROTOCOL_ERROR", e.Code);
        Assert.Equal(0, Broker.QueueDepth("pbtest.Calc.DLQ"));
    }

    [Fact]
    public async Task CallMetadataReachesTheHandler()
    {
        await ServeAsync();
        var who = await Proxy().WhoamiAsync(new Nothing(), new CallOptions { Actor = "tester", MessageId = "order-1" });
        Assert.Equal("tester", who.Actor);
        Assert.Equal("order-1", who.MessageId);
        Assert.Equal("REQUEST.pbtest.Calc.whoami", who.RoutingKey);
        Assert.False(who.Redelivered);
        // A fresh UUID otherwise.
        Assert.Equal(36, (await Proxy().WhoamiAsync(new Nothing())).MessageId.Length);
    }

    [Fact]
    public async Task CustomTypesMapsAndDefaultsRoundTrip()
    {
        await ServeAsync();
        var w = new Wallet
        {
            Amount = CustomTypes.Bigint(BigInteger.Pow(10, 30)),
            At = CustomTypes.Timestamp(DateTimeOffset.Parse("1969-07-20T20:17:40Z")),
            Kind = Kind.Spot,
            Big = 9007199254740993L,
        };
        w.Balances["k"] = CustomTypes.Bigint(BigInteger.Pow(2, 200));
        w.Parts.Add(CustomTypes.Bigint(1));
        w.Parts.Add(CustomTypes.Bigint(2));
        var back = await Proxy().EchoAsync(w);
        Assert.Equal(w, back);
        Assert.Equal(BigInteger.Pow(10, 30), CustomTypes.ToBigInteger(back.Amount));
    }

    [Fact]
    public async Task AMalformedCustomTypeInARequestIsAProtocolError()
    {
        await ServeAsync();
        // Built by hand, bypassing the client-side check, as a foreign peer could.
        var payload = new Wallet { Amount = new Protobus.Types.bigint { Value = ByteString.CopyFrom(new byte[33]) } }.ToByteArray();
        var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.echo", null, payload));
        var reply = await Ctx.PublishMessageAsync(request, "REQUEST.pbtest.Calc.echo");
        Assert.Equal("PROTOCOL_ERROR", Envelopes.DecodeResponse(reply!).Error!.Code);
    }

    [Fact]
    public async Task OneWayCallsReturnOnceConfirmed()
    {
        var s = await ServeAsync();
        var empty = await Proxy().AddAsync(Add(1, 2), new CallOptions { Rpc = false });
        Assert.Equal(new AddResponse(), empty);
        await Proxy().FailAsync(new FailRequest { Id = "1", Handled = true }, new CallOptions { Rpc = false });
        Assert.True(await Eventually(() => s.FailAttempts == 1));
    }

    [Fact]
    public async Task ACallToNoServiceIsUnroutable()
    {
        var e = await Assert.ThrowsAsync<UnroutableError>(() => Proxy().AddAsync(Add(1, 1)));
        Assert.NotNull(e.MessageId);
    }

    [Fact]
    public async Task AnUnansweredCallTimesOut()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        await Assert.ThrowsAsync<RpcTimeoutError>(() =>
            Proxy().SlowAsync(new SlowRequest { Ms = 2000 }, new CallOptions { TimeoutMs = 100 }));
    }

    [Fact]
    public async Task ACancelledCallStopsWaiting()
    {
        await ServeAsync(new MessageServiceOptions { ProcessingTimeoutMs = 60000 });
        using var cts = new System.Threading.CancellationTokenSource(100);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Proxy().SlowAsync(new SlowRequest { Ms = 2000 }, cancellationToken: cts.Token));
    }

    [Fact]
    public async Task AProcessingTimeoutIsRetriedThenAnsweredWithItsCode()
    {
        var s = await ServeAsync(new MessageServiceOptions
        {
            ProcessingTimeoutMs = 50,
            Retry = new RetryOptions(MaxRetries: 1, RetryDelayMs: 10),
        });
        var e = await Assert.ThrowsAsync<RemoteError>(() => Proxy().SlowAsync(new SlowRequest { Ms = 5000 }));
        Assert.Equal("PROCESSING_TIMEOUT", e.Code);
        Assert.Equal(2, s.SlowStarted);
        // The handler's token fired: it stopped on its own.
        Assert.True(await Eventually(() => s.SlowAborted == 2));
    }

    [Fact]
    public async Task AnInstanceNamedServiceIsReachedThroughItsName()
    {
        await ServeAsync((c, o) => new CalcService.Instance(c, o, "pbtest.Calc.player6"), null, Ctx);
        var p = Proxy("pbtest.Calc.player6");
        Assert.Equal("pbtest.Calc", p.ContractServiceName);
        Assert.Equal("REQUEST.pbtest.Calc.player6.whoami", (await p.WhoamiAsync(new Nothing())).RoutingKey);
        Assert.True(Broker.QueueExists("pbtest.Calc.player6"));
    }

    [Fact]
    public async Task TheUntypedProxyCallsByName()
    {
        await ServeAsync();
        var p = new ServiceProxy(Ctx, "pbtest.Calc");
        Ctx.Factory.Register(PbtestReflection.Descriptor);
        p.Init();
        Assert.Equal(5, (await p.CallAsync("add", Add(2, 3), AddResponse.Parser)).Result);
        var raw = await p.CallRawAsync("add", Add(4, 5).ToByteArray());
        Assert.Equal(9, AddResponse.Parser.ParseFrom(raw).Result);
        await Assert.ThrowsAsync<UnknownMethodError>(() => p.CallAsync("nope", Add(1, 1), AddResponse.Parser));
        await Assert.ThrowsAsync<InvalidRequestError>(() => p.CallAsync("divide", Add(1, 1), DivideResponse.Parser));
        await Assert.ThrowsAsync<InvalidRequestError>(() => p.CallAsync("ticks", Add(1, 1), Tick.Parser));
    }

    [Fact]
    public async Task InvalidCallOptionsAreRefusedBeforeAnythingIsSent()
    {
        await ServeAsync();
        await Assert.ThrowsAsync<InvalidPriorityError>(() => Proxy().AddAsync(Add(1, 1), new CallOptions { Priority = 256 }));
        await Assert.ThrowsAsync<InvalidMessageIdError>(() => Proxy().AddAsync(Add(1, 1), new CallOptions { MessageId = " " }));
        await Assert.ThrowsAsync<InvalidMessageIdError>(() =>
            Proxy().AddAsync(Add(1, 1), new CallOptions { MessageId = string.Concat(Enumerable.Repeat("é", 128)) }));
    }

    [Fact]
    public async Task ARequestWhoseBodyContradictsItsRoutingKeyIsRefused()
    {
        var s = await ServeAsync();
        var request = Envelopes.EncodeRequest(new Envelopes.Request("pbtest.Calc.fail", null,
            new FailRequest { Id = "x" }.ToByteArray()));
        var reply = await Ctx.PublishMessageAsync(request, "REQUEST.pbtest.Calc.add");
        var r = Envelopes.DecodeResponse(reply!);
        Assert.Equal("PROTOCOL_ERROR", r.Error!.Code);
        Assert.Equal("pbtest.Calc.add", r.Error.Method);
        Assert.Equal(0, s.FailAttempts);
    }

    [Fact]
    public async Task AnUndecodableEnvelopeIsAProtocolError()
    {
        await ServeAsync();
        var reply = await Ctx.PublishMessageAsync(new byte[] { 0x0a, 0xff }, "REQUEST.pbtest.Calc.add");
        Assert.Equal("PROTOCOL_ERROR", Envelopes.DecodeResponse(reply!).Error!.Code);
    }

    [Fact]
    public async Task AMaxPriorityQueueIsDeclaredAndHonoured()
    {
        var s = await ServeAsync(new MessageServiceOptions { MaxPriority = 2 });
        Assert.Equal(2, Convert.ToInt32(Broker.QueueArguments("pbtest.Calc")!["x-max-priority"]));
        Assert.Throws<InvalidPriorityError>(() => new CalcService(Ctx, new MessageServiceOptions { MaxPriority = 2, LateAck = false }));
        Assert.Equal(3, (await Proxy().AddAsync(Add(1, 2), new CallOptions { Priority = Config.PriorityHigh })).Result);
        Assert.Equal(0, s.FailAttempts);
    }
}
