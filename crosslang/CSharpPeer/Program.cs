// The C# participant of the cross-language suites.
//
//   CSharpPeer server   serve the interop services; prints READY
//   CSharpPeer client   run the client scenario against PEER_TARGET's services; prints PASS/FAIL
//                       lines, then DONE
//
// The broker is PROTOBUS_TEST_AMQP. Behaviour mirrors the Go, TypeScript, Python, C++ and Java
// peers exactly: the same services, the same canonical values, the same fifteen client checks.
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Interop;
using Protobus;
using Protobus.Types;

const string Lang = "csharp";

static string Env(string name, string fallback) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : fallback;

Logger.Level = Environment.GetEnvironmentVariable("PROTOBUS_TEST_LOG") != null ? LogLevel.Debug : LogLevel.Error;
try
{
    return (args.Length > 0 ? args[0] : "client") == "server" ? await Peer.ServeAsync(Lang, Env("PROTOBUS_TEST_AMQP", ""))
        : await Peer.ClientAsync(Lang, Env("PEER_TARGET", Lang), Env("PROTOBUS_TEST_AMQP", ""));
}
catch (Exception e)
{
    Console.Error.WriteLine(e);
    return 2;
}

internal static class Peer
{
    /// <summary>The Balance every peer returns for an ordinary account.</summary>
    internal static Balance Canonical()
    {
        var b = new Balance
        {
            Amount = CustomTypes.Bigint(BigInteger.Pow(10, 30)),
            AsOf = CustomTypes.TimestampMillis(1577836800000L), // 2020-01-01T00:00:00Z
            Big = 9007199254740993L,
            Kind = Kind.Future,
            Inner = new Inner { Name = "root", Value = CustomTypes.Bigint(7) },
            Ubig = ulong.MaxValue,
            Blob = ByteString.CopyFrom(0, 1, 0xff),
            Ratio = 0.5,
            Flag = true,
            Neg = -5,
            BeforeEpoch = CustomTypes.TimestampMillis(-14182940000L), // 1969-07-20T20:17:40Z
            Zero = 0,
        };
        b.Tags.Add(new[] { "a", "b" });
        b.Counts["x"] = 1;
        b.Counts["y"] = 2;
        b.Balances["k"] = CustomTypes.Bigint(BigInteger.Pow(2, 200));
        b.Parts.Add(new[] { CustomTypes.Bigint(1), CustomTypes.Bigint(2), CustomTypes.Bigint(3) });
        b.Inner.Children.Add(new Inner { Name = "leaf", Value = CustomTypes.Bigint(8) });
        return b;
    }

    // ---- server ------------------------------------------------------------------------

    private static readonly object ProducedLock = new();
    private static Produced produced = new();

    private static void Record(Action<Produced> f)
    {
        lock (ProducedLock)
        {
            var p = produced.Clone();
            f(p);
            produced = p;
        }
    }

    private sealed class Counter : CounterProtobus.Base
    {
        private readonly string name;
        private readonly string lang;

        public Counter(Context ctx, string name, string lang) : base(ctx)
        {
            this.name = name;
            this.lang = lang;
        }

        public override string ServiceName => name;

        public override Task<AddResponse> Add(AddRequest r, CallContext ctx) => Task.FromResult(new AddResponse { Sum = r.A + r.B });

        public override async IAsyncEnumerable<Tick> Tick(TickRequest r, CallContext ctx)
        {
            if (r.EmitNothing) yield break;
            Record(p =>
            {
                p.Yielded = 0;
                p.StoppedEarly = false;
                p.Finished = false;
            });
            for (var i = 0; i < r.Count; i++)
            {
                if (r.FailAt > 0 && i >= r.FailAt)
                {
                    if (r.Unhandled) throw new InvalidOperationException("stream broke");
                    throw new HandledError("deliberate failure at chunk " + i, "TEST_FAIL");
                }
                if (ctx.CancellationToken.IsCancellationRequested)
                {
                    Record(p => p.StoppedEarly = true);
                    yield break;
                }
                yield return new Tick { Seq = i, Payload = "chunk-" + i };
                Record(p => p.Yielded++);
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
                        Record(p => p.StoppedEarly = true);
                        yield break;
                    }
                }
            }
            Record(p => p.Finished = true);
        }

        public override Task<Produced> Produced(Nothing r, CallContext ctx)
        {
            lock (ProducedLock) return Task.FromResult(produced.Clone());
        }

        public override Task<Who> Whoami(Nothing r, CallContext ctx) => Task.FromResult(new Who
        {
            Actor = ctx.Actor,
            MessageId = ctx.MessageId ?? "",
            RoutingKey = ctx.RoutingKey,
            Lang = lang,
        });
    }

    private sealed class Wallet : WalletProtobus.Base
    {
        public Wallet(Context ctx, MessageServiceOptions o) : base(ctx, o) { }

        public override Task<Balance> Balance(Query q, CallContext ctx) => q.Account switch
        {
            "boom" => throw new HandledError("no such account", "NOT_FOUND"),
            "crash" => throw new InvalidOperationException("kaboom"),
            _ => Task.FromResult(Canonical()),
        };

        public override Task<Balance> Echo(Balance b, CallContext ctx) => Task.FromResult(b);
    }

    private sealed class Flaky : FlakyProtobus.Base
    {
        private readonly string lang;

        public Flaky(Context ctx, MessageServiceOptions o, string lang) : base(ctx, o) => this.lang = lang;

        public override async Task<Nothing> Fail(FailRequest r, CallContext ctx)
        {
            await PublishEventAsync(new Attempted { Lang = lang, MessageId = ctx.MessageId ?? "" }, "EVENT.attempted");
            throw new InvalidOperationException("flaky " + lang);
        }
    }

    private sealed class Listener : ListenerProtobus.Base
    {
        private readonly string lang;

        public Listener(Context ctx, MessageServiceOptions o, string lang) : base(ctx, o) => this.lang = lang;

        public override string ServiceName => "interop.Listener." + lang;
    }

    internal static async Task<int> ServeAsync(string lang, string amqp)
    {
        var ctx = new Context();
        await ctx.InitAsync(amqp);
        var noRetry = new MessageServiceOptions { Retry = new RetryOptions(MaxRetries: 0) };
        // Shared with the other languages' replicas: the retry arguments must match theirs.
        var flaky = new MessageServiceOptions { Retry = new RetryOptions(MaxRetries: 3, RetryDelayMs: 100), MaxConcurrent = 4 };
        await new Counter(ctx, "interop.Counter", lang).InitAsync();
        await new Counter(ctx, "interop.Counter.inst1", lang).InitAsync();
        await new Wallet(ctx, noRetry).InitAsync();
        await new Flaky(ctx, flaky, lang).InitAsync();
        var listener = new Listener(ctx, noRetry, lang);
        await listener.InitAsync();
        await listener.SubscribeEventAsync<Ping>((ping, _, _) => ctx.PublishEventAsync(new Ping
        {
            Id = "pong:" + ping.Id,
            N = CustomTypes.Bigint(CustomTypes.ToBigInteger(ping.N) + 1),
            From = lang,
        }, "EVENT.pong." + lang), "EVENT.ping." + lang);
        Console.WriteLine("READY");
        Console.Out.Flush();
        var stop = new TaskCompletionSource();
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, c =>
        {
            c.Cancel = true;
            stop.TrySetResult();
        });
        await stop.Task;
        await ctx.DisposeAsync();
        return 0;
    }

    // ---- client ------------------------------------------------------------------------

    private static readonly List<string> Failures = new();

    private static async Task Check(string name, Func<Task> fn)
    {
        try
        {
            await fn().WaitAsync(TimeSpan.FromSeconds(30));
            Console.WriteLine("PASS " + name);
        }
        catch (Exception e)
        {
            Console.WriteLine(("FAIL " + name + ": " + e.GetType().Name + ": " + e.Message).Replace('\n', ' '));
            Failures.Add(name);
        }
        Console.Out.Flush();
    }

    private static void Require(bool cond, string msg)
    {
        if (!cond) throw new Exception(msg);
    }

    /// <summary>bigints compared by value: a peer may encode one shorter than 32 bytes.</summary>
    internal static IMessage Normalize(IMessage m)
    {
        if (m is bigint b) return CustomTypes.Bigint(CustomTypes.ToBigInteger(b));
        var copy = m.Descriptor.Parser.ParseFrom(m.ToByteString());
        foreach (var f in copy.Descriptor.Fields.InFieldNumberOrder())
        {
            if (f.FieldType != FieldType.Message) continue;
            var value = f.Accessor.GetValue(copy);
            if (f.IsMap)
            {
                var map = (IDictionary)value;
                if (f.MessageType.FindFieldByNumber(2).FieldType != FieldType.Message) continue;
                foreach (var key in map.Keys.Cast<object>().ToList()) map[key] = Normalize((IMessage)map[key]!);
            }
            else if (f.IsRepeated)
            {
                var list = (IList)value;
                for (var i = 0; i < list.Count; i++) list[i] = Normalize((IMessage)list[i]!);
            }
            else if (value is IMessage inner)
            {
                f.Accessor.SetValue(copy, Normalize(inner));
            }
        }
        return copy;
    }

    private static void Eq(object got, object want, string what)
    {
        var g = got is IMessage gm ? Normalize(gm) : got;
        var w = want is IMessage wm ? Normalize(wm) : want;
        var same = g is IEnumerable ge && g is not string && w is IEnumerable we
            ? ge.Cast<object>().SequenceEqual(we.Cast<object>())
            : Equals(g, w);
        Require(same, $"{what}: got {Show(g)}, want {Show(w)}");
    }

    private static string Show(object o) => o is IEnumerable e && o is not string and not IMessage
        ? "[" + string.Join(", ", e.Cast<object>()) + "]"
        : o.ToString() ?? "null";

    private static async Task<RemoteError> ExpectRemote(Func<Task> fn)
    {
        try
        {
            await fn();
        }
        catch (RemoteError e)
        {
            return e;
        }
        throw new Exception("expected a RemoteError");
    }

    internal static async Task<int> ClientAsync(string lang, string target, string amqp)
    {
        var ctx = new Context();
        await ctx.InitAsync(amqp);
        var counter = new CounterProtobus.Proxy(ctx);
        counter.Init();
        var inst = new CounterProtobus.Proxy(ctx, "interop.Counter.inst1");
        inst.Init();
        var wallet = new WalletProtobus.Proxy(ctx);
        wallet.Init();

        await Check("unary", async () => Eq((await counter.AddAsync(new AddRequest { A = 2, B = 3 })).Sum, 5, "add"));
        await Check("priority on a plain queue", async () =>
            Eq((await counter.AddAsync(new AddRequest { A = 1, B = 1 }, new CallOptions { Priority = 2 })).Sum, 2, "add"));
        await Check("stream in order", async () =>
        {
            var seqs = new List<int>();
            var payloads = new List<string>();
            await foreach (var t in counter.Tick(new TickRequest { Count = 5 }))
            {
                seqs.Add(t.Seq);
                payloads.Add(t.Payload);
            }
            Eq(seqs, new[] { 0, 1, 2, 3, 4 }, "seq");
            Eq(payloads, new[] { "chunk-0", "chunk-1", "chunk-2", "chunk-3", "chunk-4" }, "payload");
        });
        await Check("empty stream", async () =>
        {
            var n = 0;
            await foreach (var _ in counter.Tick(new TickRequest { EmitNothing = true })) n++;
            Eq(n, 0, "chunks");
        });
        await Check("mid-stream handled error", async () =>
        {
            var got = new List<Tick>();
            var e = await ExpectRemote(async () =>
            {
                await foreach (var t in counter.Tick(new TickRequest { Count = 10, FailAt = 2 })) got.Add(t);
            });
            Eq(e.Code ?? "", "TEST_FAIL", "code");
            Require(e.Message.Contains("deliberate failure at chunk 2"), e.Message);
            Eq(got.Count, 2, "chunks before the error");
        });
        await Check("mid-stream unhandled error", async () =>
        {
            var e = await ExpectRemote(async () =>
            {
                await foreach (var _ in counter.Tick(new TickRequest { Count = 10, FailAt = 1, Unhandled = true })) { }
            });
            Eq(e.Message, "stream broke", "message");
        });
        await Check("cancellation reaches the producer", async () =>
        {
            var n = 0;
            await foreach (var _ in counter.Tick(new TickRequest { Count = 500, DelayMs = 10 }))
                if (++n == 3) break;
            Produced? p = null;
            for (var i = 0; i < 100; i++)
            {
                p = await counter.ProducedAsync(new Nothing());
                if (p.StoppedEarly) break;
                await Task.Delay(50);
            }
            Require(p!.StoppedEarly && !p.Finished && p.Yielded < 500, "produced " + p);
        });
        await Check("custom types, defaults and maps", async () =>
            Eq(await wallet.BalanceAsync(new Query { Account = "acc" }), Canonical(), "balance"));
        await Check("echo round trip", async () => Eq(await wallet.EchoAsync(Canonical()), Canonical(), "echo"));
        await Check("handled error", async () =>
        {
            var e = await ExpectRemote(() => wallet.BalanceAsync(new Query { Account = "boom" }));
            Eq(e.Code ?? "", "NOT_FOUND", "code");
            Eq(e.Message, "no such account", "message");
        });
        await Check("unhandled error", async () =>
        {
            var e = await ExpectRemote(() => wallet.BalanceAsync(new Query { Account = "crash" }));
            Eq(e.Message, "kaboom", "message");
        });
        await Check("unimplemented method", async () =>
        {
            var e = await ExpectRemote(() => counter.UnimplementedAsync(new Nothing()));
            Eq(e.Code ?? "", "PROTOCOL_ERROR", "code");
        });
        await Check("call metadata", async () =>
        {
            var who = await counter.WhoamiAsync(new Nothing(), new CallOptions { Actor = "client-" + lang, MessageId = "mid-" + lang + "-1" });
            Eq(who, new Who { Actor = "client-" + lang, MessageId = "mid-" + lang + "-1", RoutingKey = "REQUEST.interop.Counter.whoami", Lang = target },
                "who");
        });
        await Check("instance routing", async () =>
            Eq((await inst.WhoamiAsync(new Nothing())).RoutingKey, "REQUEST.interop.Counter.inst1.whoami", "routing key"));
        await Check("events both ways", async () =>
        {
            var listener = new EventListener(ctx.Connection);
            await listener.InitAsync(null, "");
            var got = new TaskCompletionSource<Ping>(TaskCreationOptions.RunContinuationsAsynchronously);
            await listener.SubscribeAsync<Ping>((e, _, _) =>
            {
                got.TrySetResult(e);
                return Task.CompletedTask;
            }, "EVENT.pong." + target);
            await listener.StartAsync();
            var n = BigInteger.Pow(2, 70);
            await ctx.PublishEventAsync(new Ping { Id = lang + "-1", N = CustomTypes.Bigint(n), From = lang }, "EVENT.ping." + target);
            var pong = await got.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Eq(pong, new Ping { Id = "pong:" + lang + "-1", N = CustomTypes.Bigint(n + 1), From = target }, "pong");
            await listener.CloseAsync();
        });

        Console.WriteLine("DONE");
        Console.Out.Flush();
        await ctx.DisposeAsync();
        return Failures.Count == 0 ? 0 : 1;
    }
}
