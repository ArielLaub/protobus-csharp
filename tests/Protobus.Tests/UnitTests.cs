using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Google.Protobuf;
using Pbtest;
using Protobus.Internal;
using Protobus.Types;
using Xunit;

namespace Protobus.Tests;

/// <summary>
/// The golden values were produced by the TypeScript reference implementation (protobus 2.4.0,
/// MessageFactory); protobus-go, protobus-cpp and protobus-java pin the same bytes. They keep this
/// encoder byte for byte with what a TypeScript peer emits.
/// </summary>
public class EnvelopeTests
{
    private static byte[] Hex(string h) => Convert.FromHexString(h);
    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    [Fact]
    public void RequestMatchesTypeScript()
    {
        Assert.Equal("0a09542e5376632e6164641a020801", Hex(Envelopes.EncodeRequest(new Envelopes.Request("T.Svc.add", null, Hex("0801")))));
        // proto3 would drop an all-default payload; TypeScript always writes data, even empty.
        Assert.Equal("0a09542e5376632e6164641201781a00", Hex(Envelopes.EncodeRequest(new Envelopes.Request("T.Svc.add", "x", Array.Empty<byte>()))));
    }

    [Fact]
    public void ResponseMatchesTypeScript()
    {
        Assert.Equal("0a0f0a09542e5376632e61646412020802",
            Hex(Envelopes.EncodeResponse(new Envelopes.Response(new Envelopes.Result("T.Svc.add", Hex("0802")), null))));
        Assert.Equal("0a0d0a09542e5376632e6164641200",
            Hex(Envelopes.EncodeResponse(new Envelopes.Response(new Envelopes.Result("T.Svc.add", Array.Empty<byte>()), null))));
        // An empty code is written explicitly, as TypeScript does.
        Assert.Equal("12130a09542e5376632e6164641204626f6f6d1a00",
            Hex(Envelopes.EncodeResponse(new Envelopes.Response(null, new Envelopes.ErrorReply("T.Svc.add", "boom", "")))));
        Assert.Equal("12110a09542e5376632e61646412016d1a0143",
            Hex(Envelopes.EncodeResponse(new Envelopes.Response(null, new Envelopes.ErrorReply("T.Svc.add", "m", "C")))));
    }

    [Fact]
    public void ResponseMustCarryExactlyOneMember()
    {
        Assert.Throws<ArgumentException>(() => Envelopes.EncodeResponse(new Envelopes.Response(null, null)));
        Assert.Throws<ArgumentException>(() => Envelopes.EncodeResponse(new Envelopes.Response(
            new Envelopes.Result("a", Array.Empty<byte>()), new Envelopes.ErrorReply("a", "", ""))));
    }

    [Fact]
    public void EventMatchesTypeScript() =>
        Assert.Equal("0a04542e4576120a4556454e542e542e45761a030a0179",
            Hex(Envelopes.EncodeEvent(new Envelopes.Event("T.Ev", "EVENT.T.Ev", Hex("0a0179")))));

    [Fact]
    public void DecodesTypeScriptRequest()
    {
        var r = Envelopes.DecodeRequest(Hex("0a09542e5376632e61646412001a020801"));
        Assert.Equal("T.Svc.add", r.Method);
        Assert.Equal("", r.Actor);
        Assert.Equal(Hex("0801"), r.Data);
    }

    [Fact]
    public void DecodesTypeScriptResponses()
    {
        var e = Envelopes.DecodeResponse(Hex("12110a09542e5376632e61646412016d1a0143"));
        Assert.Null(e.Result);
        Assert.Equal(new Envelopes.ErrorReply("T.Svc.add", "m", "C"), e.Error);
        var r = Envelopes.DecodeResponse(Hex("0a0d0a09542e5376632e6164641200"));
        Assert.Equal("T.Svc.add", r.Result!.Method);
        Assert.Empty(r.Result.Data);
    }

    [Fact]
    public void ErrorWinsOverResult()
    {
        // TypeScript checks `error` first; a container carrying both must read as an error on
        // every port.
        var w = new Envelopes.Writer();
        w.Bytes(1, Envelopes.EncodeResult(new Envelopes.Result("a.B.c", Array.Empty<byte>())));
        var err = new Envelopes.Writer();
        err.String(1, "a.B.c");
        err.String(2, "no");
        w.Bytes(2, err.ToArray());
        var r = Envelopes.DecodeResponse(w.ToArray());
        Assert.NotNull(r.Error);
        Assert.Null(r.Result);
    }

    [Fact]
    public void EmptyResponseIsRefused() =>
        Assert.Throws<Envelopes.MalformedException>(() => Envelopes.DecodeResponse(Array.Empty<byte>()));

    [Fact]
    public void DecodesTypeScriptEvent()
    {
        var e = Envelopes.DecodeEvent(Hex("0a04542e4576120a4556454e542e542e45761a030a0179"));
        Assert.Equal("T.Ev", e.Type);
        Assert.Equal("EVENT.T.Ev", e.Topic);
        Assert.Equal(Hex("0a0179"), e.Data);
    }

    [Fact]
    public void SkipsUnknownFields()
    {
        // A future peer adding a field must not break an older reader.
        var b = Envelopes.EncodeRequest(new Envelopes.Request("a.B.c", null, new byte[] { 1 }))
            .Concat(new byte[] { (99 << 3) & 0x7f | 0x80, 99 >> 4, 7 }).ToArray();
        Assert.Equal("a.B.c", Envelopes.DecodeRequest(b).Method);
    }

    [Theory]
    [InlineData("0a0954")]
    [InlineData("00")]
    [InlineData("0801")]
    [InlineData("0aff")]
    [InlineData("0a01ff")]
    public void RejectsMalformedInput(string h) =>
        Assert.Throws<Envelopes.MalformedException>(() => Envelopes.DecodeRequest(Hex(h)));

    [Fact]
    public void RoundTrips()
    {
        var data = Enumerable.Repeat((byte)0xab, 300).ToArray();
        var o = Envelopes.DecodeRequest(Envelopes.EncodeRequest(new Envelopes.Request("pkg.sub.Service.method", "user:42", data)));
        Assert.Equal("pkg.sub.Service.method", o.Method);
        Assert.Equal("user:42", o.Actor);
        Assert.Equal(data, o.Data);
    }

    [Fact]
    public void RandomBytesNeverEscapeAsAnythingButMalformed()
    {
        // A property test over the decoders: arbitrary input either decodes or is refused as
        // malformed, never with some other exception.
        var random = new Random(42);
        for (var i = 0; i < 20000; i++)
        {
            var b = new byte[random.Next(40)];
            random.NextBytes(b);
            foreach (var decode in new Action<byte[]>[] { x => Envelopes.DecodeRequest(x), x => Envelopes.DecodeResponse(x), x => Envelopes.DecodeEvent(x) })
            {
                try
                {
                    decode(b);
                }
                catch (Envelopes.MalformedException)
                {
                    // Refused.
                }
            }
        }
    }
}

public class CustomTypeTests
{
    [Fact]
    public void BigintIsThirtyTwoBigEndianBytes()
    {
        var one = CustomTypes.Bigint(1);
        var expected = new byte[32];
        expected[31] = 1;
        Assert.Equal(expected, one.Value.ToByteArray());
        Assert.Equal(BigInteger.One, CustomTypes.ToBigInteger(one));
        Assert.Equal(CustomTypes.BigintMax, CustomTypes.ToBigInteger(CustomTypes.Bigint(CustomTypes.BigintMax)));
        Assert.Equal(new BigInteger(255), CustomTypes.ToBigInteger(CustomTypes.Bigint("0xff")));
    }

    [Fact]
    public void BigintRefusesWhatNoPortCanCarry()
    {
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.Bigint(BigInteger.MinusOne));
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.Bigint(CustomTypes.BigintMax + 1));
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.Bigint("12x"));
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.ToBigInteger(new bigint { Value = ByteString.CopyFrom(new byte[33]) }));
    }

    [Fact]
    public void AnUnsetOrShortBigintDecodes()
    {
        Assert.Equal(BigInteger.Zero, CustomTypes.ToBigInteger(new bigint()));
        Assert.Equal(new BigInteger(256), CustomTypes.ToBigInteger(new bigint { Value = ByteString.CopyFrom(1, 0) }));
    }

    [Fact]
    public void TimestampIsSignedMillis()
    {
        var before = DateTimeOffset.Parse("1969-07-20T20:17:40Z");
        var t = CustomTypes.Timestamp(before);
        Assert.Equal(before.ToUnixTimeMilliseconds(), t.Value);
        Assert.Equal(before, CustomTypes.ToDateTimeOffset(t));
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.TimestampMillis(8_640_000_000_000_001L));
    }

    [Fact]
    public void ValidationWalksMapsRepeatedFieldsAndNesting()
    {
        var ok = new Wallet { Amount = CustomTypes.Bigint(5) };
        ok.Balances["k"] = CustomTypes.Bigint(7);
        ok.Parts.Add(CustomTypes.Bigint(1));
        CustomTypes.Validate(ok);
        var wide = new bigint { Value = ByteString.CopyFrom(new byte[40]) };
        var inMap = new Wallet();
        inMap.Balances["k"] = wide;
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.Validate(inMap));
        var inList = new Wallet();
        inList.Parts.Add(wide);
        Assert.Throws<CustomTypeRangeError>(() => CustomTypes.Validate(inList));
        Assert.Throws<CustomTypeRangeError>(() => MessageFactory.EncodeMessage(new Wallet { At = new timestamp { Value = long.MaxValue } }));
    }
}

public class ErrorTests : IDisposable
{
    public void Dispose()
    {
        Config.Reset();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void SummariesNeverCarryAnUnhandledMessage()
    {
        Assert.Equal("InvalidOperationException", Errors.SafeErrorSummary(new InvalidOperationException("password=hunter2")));
        Assert.Equal("RpcTimeoutError[RPC_TIMEOUT]", Errors.SafeErrorSummary(new RpcTimeoutError("secret")));
        Assert.Equal("HandledError[REFUSED]: refused 7", Errors.SafeErrorSummary(new HandledError("refused 7", "REFUSED")));
        Assert.Equal("ProtocolError[PROTOCOL_ERROR]: bad", Errors.SafeErrorSummary(new ProtocolError("bad")));
        Assert.Equal("UnknownError", Errors.SafeErrorSummary(null));
    }

    [Fact]
    public void HandledErrorsDefaultTheirCode()
    {
        Assert.Equal("HANDLED_ERROR", new HandledError("x").Code);
        Assert.Equal("HANDLED_ERROR", new HandledError("x", "").Code);
        Assert.Equal("PROTOCOL_ERROR", new InvalidMethodError("x").Code);
        Assert.True(Errors.IsHandledError(new InvalidMethodError("x")));
    }

    [Fact]
    public void SanitisingKeepsHandledErrorsAndTimeouts()
    {
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false");
        var handled = new HandledError("no", "NO");
        Assert.Same(handled, Errors.SanitizeErrorForClient(handled, "c1"));
        var timeout = new TimeoutError("took too long");
        Assert.Same(timeout, Errors.SanitizeErrorForClient(timeout, "c1"));
        var hidden = Errors.SanitizeErrorForClient(new InvalidOperationException("db password"), "c1");
        Assert.Equal("internal service error (correlationId c1)", hidden.Message);
        Assert.Equal("INTERNAL_ERROR", Errors.CodeOf(hidden));
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "true");
        var raw = new InvalidOperationException("x");
        Assert.Same(raw, Errors.SanitizeErrorForClient(raw, "c1"));
    }

    [Fact]
    public void UrlsAreRedacted()
    {
        Assert.Equal("amqp://user:***@host:5672/%2f", Logger.RedactUrl("amqp://user:s3cret@host:5672/%2f"));
        Assert.Equal("amqp://host/", Logger.RedactUrl("amqp://host/"));
        Assert.Equal("amqps://u:***@h/?heartbeat=5", Logger.RedactUrl("amqps://u:p@h/?heartbeat=5"));
        Assert.Equal("<redacted>", Logger.RedactUrl("not a url with spaces"));
    }
}

public class ConfigTests : IDisposable
{
    public void Dispose()
    {
        Config.Reset();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void DefaultsMatchTheOtherPorts()
    {
        Assert.Equal("proto.bus", Config.BusExchangeName);
        Assert.Equal("proto.bus.callback", Config.CallbacksExchangeName);
        Assert.Equal("proto.bus.cancel", Config.CancelExchangeName);
        Assert.Equal("proto.bus.events", Config.EventsExchangeName);
        Assert.Equal(600000, Config.MessageProcessingTimeout);
        Assert.Equal(600000, Config.RpcCallTimeoutMs);
        Assert.Equal(60000, Config.StreamIdleTimeoutMs);
        Assert.Equal(1, Config.DefaultPrefetch);
        Assert.Equal(30000, Config.PublishConfirmTimeoutMs);
        Assert.Equal(30, Config.HeartbeatSeconds);
        Assert.Equal(256, Config.MaxOutstandingConfirms);
        Assert.True(Config.ExposeInternalErrors);
    }

    [Theory]
    [InlineData("6oo000")]
    [InlineData("123abc")]
    [InlineData("-5")]
    [InlineData("0")]
    [InlineData("1.5")]
    [InlineData(" ")]
    [InlineData("")]
    [InlineData("99999999999999999999")]
    public void IntegersAreStrict(string bad)
    {
        Config.Set("MESSAGE_PROCESSING_TIMEOUT", bad);
        Assert.Equal(600000, Config.MessageProcessingTimeout);
    }

    [Fact]
    public void SurroundingSpaceIsTrimmed()
    {
        Config.Set("MESSAGE_PROCESSING_TIMEOUT", " 1500 ");
        Assert.Equal(1500, Config.MessageProcessingTimeout);
    }

    [Fact]
    public void AChangedValueIsPickedUp()
    {
        Config.Set("RPC_CALL_TIMEOUT_MS", "100");
        Assert.Equal(100, Config.RpcCallTimeoutMs);
        Config.Set("RPC_CALL_TIMEOUT_MS", "200");
        Assert.Equal(200, Config.RpcCallTimeoutMs);
    }

    [Fact]
    public void BooleansNeedAnExplicitWord()
    {
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "OFF");
        Assert.False(Config.ExposeInternalErrors);
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "nope");
        Assert.True(Config.ExposeInternalErrors);
        Config.Set("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "0");
        Assert.False(Config.ExposeInternalErrors);
    }

    [Fact]
    public void EmptyExchangeNameKeepsTheDefault()
    {
        Config.Set("BUS_EXCHANGE_NAME", "");
        Assert.Equal("proto.bus", Config.BusExchangeName);
        Config.Set("BUS_EXCHANGE_NAME", "other.bus");
        Assert.Equal("other.bus", Config.BusExchangeName);
    }
}

public class TrieTests
{
    private static List<string> Match(Trie<string> t, string topic) => t.Match(topic);

    [Fact]
    public void ExactWordsMatch()
    {
        var t = new Trie<string>();
        t.Add("EVENT.a.b", "x");
        Assert.Equal(new[] { "x" }, Match(t, "EVENT.a.b"));
        Assert.Empty(Match(t, "EVENT.a"));
        Assert.Empty(Match(t, "EVENT.a.b.c"));
    }

    [Fact]
    public void StarIsExactlyOneWord()
    {
        var t = new Trie<string>();
        t.Add("EVENT.*.b", "x");
        Assert.Equal(new[] { "x" }, Match(t, "EVENT.a.b"));
        Assert.Empty(Match(t, "EVENT.b"));
        Assert.Empty(Match(t, "EVENT.a.c.b"));
    }

    [Fact]
    public void HashIsZeroOrMoreWords()
    {
        var t = new Trie<string>();
        t.Add("EVENT.#", "x");
        Assert.Equal(new[] { "x" }, Match(t, "EVENT"));
        Assert.Equal(new[] { "x" }, Match(t, "EVENT.a"));
        Assert.Equal(new[] { "x" }, Match(t, "EVENT.a.b.c"));
        var mid = new Trie<string>();
        mid.Add("a.#.c", "y");
        Assert.Equal(new[] { "y" }, Match(mid, "a.c"));
        Assert.Equal(new[] { "y" }, Match(mid, "a.b.c"));
        Assert.Equal(new[] { "y" }, Match(mid, "a.b.b.c"));
        Assert.Empty(Match(mid, "a.b"));
        var all = new Trie<string>();
        all.Add("#", "z");
        Assert.Equal(new[] { "z" }, Match(all, "anything.at.all"));
    }

    [Fact]
    public void AShorterPatternStillMatchesBesideALongerOne()
    {
        var t = new Trie<string>();
        t.Add("a.b", "short");
        t.Add("a.b.c", "long");
        Assert.Equal(new[] { "short" }, Match(t, "a.b"));
        Assert.Equal(new[] { "long" }, Match(t, "a.b.c"));
        // A node that is only a step along a longer pattern does not match.
        Assert.Empty(Match(t, "a"));
    }

    [Fact]
    public void SeveralValuesOnOnePatternAllMatchOnce()
    {
        var t = new Trie<string>();
        t.Add("EVENT.x", "first");
        t.Add("EVENT.x", "second");
        t.Add("EVENT.*", "star");
        t.Add("#", "hash");
        Assert.Equal(new[] { "first", "second", "star", "hash" }, Match(t, "EVENT.x"));
    }
}
