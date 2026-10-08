using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Pbtest;
using Protobus.Amqp;
using Headers = Protobus.Internal.Headers;
using RabbitMQ.Client;
using Xunit;

namespace Protobus.IntegrationTests;

/// <summary>The suites that need a real RabbitMQ.</summary>
public class IntegrationTests : IAsyncLifetime
{
    private readonly List<Context> contexts = new();
    private string name = "";

    /// <summary>A Calc service under a unique instance name, so suites never share queues.</summary>
    private class Calc : CalcProtobus.Base
    {
        private readonly string name;
        public int FailAttempts;
        public int Stopped;
        public readonly ConcurrentQueue<string> Order = new();

        public Calc(Context c, MessageServiceOptions? o, string name) : base(c, o) => this.name = name;

        public override string ServiceName => name;

        public override Task<AddResponse> Add(AddRequest r, CallContext ctx) => Task.FromResult(new AddResponse { Result = r.A + r.B });

        public override Task<Nothing> Fail(FailRequest r, CallContext ctx)
        {
            Interlocked.Increment(ref FailAttempts);
            if (r.Handled) throw new HandledError("refused", "REFUSED");
            throw new InvalidOperationException("boom");
        }

        public override async IAsyncEnumerable<Tick> Ticks(TickRequest r, CallContext ctx)
        {
            for (var i = 0; i < r.Count; i++)
            {
                if (ctx.CancellationToken.IsCancellationRequested)
                {
                    Interlocked.Increment(ref Stopped);
                    yield break;
                }
                yield return new Tick { Seq = i };
                if (r.DelayMs > 0)
                {
                    try
                    {
                        await Task.Delay(r.DelayMs, ctx.CancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // Seen at the top of the loop.
                    }
                }
            }
        }

        public override Task<Who> Whoami(Nothing r, CallContext ctx)
        {
            Order.Enqueue(ctx.Actor);
            return Task.FromResult(new Who { Actor = ctx.Actor, MessageId = ctx.MessageId ?? "", RoutingKey = ctx.RoutingKey });
        }

        public override Task<Wallet> Echo(Wallet w, CallContext ctx) => Task.FromResult(w);
    }

    public ValueTask InitializeAsync()
    {
        RealBroker.AmqpUrl();
        Config.Reset();
        Config.Set("RPC_CALL_TIMEOUT_MS", "15000");
        if (Environment.GetEnvironmentVariable("PROTOBUS_TEST_LOG") == null) Logger.Level = LogLevel.Silent;
        name = "pbtest.Calc.it" + Guid.NewGuid().ToString("N").Substring(0, 8);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var c in contexts) await c.DisposeAsync();
        await RealBroker.DeleteServiceAsync(name);
        Config.Reset();
        Logger.Level = LogLevel.Info;
        GC.SuppressFinalize(this);
    }

    private async Task<Context> ContextAsync()
    {
        var c = new Context(new ContextOptions { Reconnection = new ReconnectionOptions(InitialDelayMs: 100, MaxDelayMs: 500) });
        await c.InitAsync(RealBroker.AmqpUrl());
        contexts.Add(c);
        return c;
    }

    private async Task<Calc> ServeAsync(Context c, MessageServiceOptions? o = null)
    {
        var s = new Calc(c, o, name);
        await s.InitAsync();
        return s;
    }

    private CalcProtobus.Proxy Proxy(Context c)
    {
        var p = new CalcProtobus.Proxy(c, name);
        p.Init();
        return p;
    }

    [Fact]
    public async Task UnaryStreamingAndCustomTypesOverRabbitMq()
    {
        var c = await ContextAsync();
        await ServeAsync(c, new MessageServiceOptions { MaxConcurrent = 4 });
        var p = Proxy(c);
        Assert.Equal(42, (await p.AddAsync(new AddRequest { A = 40, B = 2 })).Result);
        var who = await p.WhoamiAsync(new Nothing(), new CallOptions { Actor = "it", MessageId = "m-1" });
        Assert.Equal("it", who.Actor);
        Assert.Equal("m-1", who.MessageId);
        Assert.Equal("REQUEST." + name + ".whoami", who.RoutingKey);
        var seqs = new List<int>();
        await foreach (var t in p.Ticks(new TickRequest { Count = 20 })) seqs.Add(t.Seq);
        Assert.Equal(Enumerable.Range(0, 20), seqs);
        var w = new Wallet
        {
            Amount = CustomTypes.Bigint(CustomTypes.BigintMax),
            At = CustomTypes.Timestamp(DateTimeOffset.Parse("1969-07-20T20:17:40Z")),
        };
        w.Balances["k"] = CustomTypes.Bigint(BigInteger.Pow(2, 200));
        Assert.Equal(w, await p.EchoAsync(w));
    }

    [Fact]
    public async Task TheRetryLadderRunsOnRealTtlAndDeadLettering()
    {
        var c = await ContextAsync();
        var s = await ServeAsync(c, new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 100) });
        var p = Proxy(c);
        var e = await Assert.ThrowsAsync<RemoteError>(() =>
            p.FailAsync(new FailRequest { Id = "r" }, new CallOptions { Priority = 1, MessageId = "ladder-1" }));
        Assert.Equal("boom", e.Message);
        Assert.Equal(4, s.FailAttempts);
        Assert.True(await RealBroker.WaitForAsync(async () => await RealBroker.QueueMessagesAsync(name + ".DLQ") == 1,
            TimeSpan.FromSeconds(10)));
        // Read the dead-letter copy as the broker stores it.
        var f = new ConnectionFactory { Uri = new Uri(RealBroker.AmqpUrl()) };
        if (string.IsNullOrEmpty(f.VirtualHost)) f.VirtualHost = "/";
        await using var raw = await f.CreateConnectionAsync();
        await using var ch = await raw.CreateChannelAsync();
        var dead = await ch.BasicGetAsync(name + ".DLQ", true);
        Assert.NotNull(dead);
        var props = dead!.BasicProperties;
        var h = props.Headers!;
        Assert.Equal(3L, Headers.Integer(h["x-retry-count"]));
        Assert.Equal("REQUEST." + name + ".fail", Headers.Text(h["x-original-routing-key"]));
        Assert.Equal(name, Headers.Text(h["x-original-queue"]));
        Assert.Equal("InvalidOperationException", Headers.Text(h["x-last-error"]));
        Assert.True(Headers.Integer(h["x-dlq-time"]) > 0);
        Assert.Equal("ladder-1", props.MessageId);
        Assert.Equal(1, props.Priority);
        Assert.Equal("application/octet-stream", props.ContentType);
        Assert.Equal(DeliveryModes.Persistent, props.DeliveryMode);
    }

    [Fact]
    public async Task ACallToNoServiceIsUnroutableAtOnce()
    {
        var c = await ContextAsync();
        var p = Proxy(c);
        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAsync<UnroutableError>(() => p.AddAsync(new AddRequest()));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ReturnsAreMatchedToTheirPublishWhenMessageIdsRepeat()
    {
        var c = await ContextAsync();
        var conn = c.Connection;
        var ch = await conn.OpenChannelAsync();
        // Durable: RabbitMQ 4 refuses a transient queue that is not exclusive.
        await conn.DeclareQueueAsync(ch, name, true, false, false);
        var exchange = name + ".x";
        await conn.DeclareExchangeAsync(ch, exchange, "topic");
        await ch.BindQueueAsync(name, exchange, "routed");
        try
        {
            var same = new MessageProperties { MessageId = "same-id" };
            var routed = new List<Task<string>>();
            var unroutable = new List<Task<string>>();
            for (var i = 0; i < 50; i++)
            {
                routed.Add(conn.PublishAsync(ch, exchange, "routed", new byte[] { 1 }, new PublishOptions(same, true)));
                unroutable.Add(conn.PublishAsync(ch, exchange, "nowhere", new byte[] { 2 }, new PublishOptions(same, true)));
            }
            foreach (var f in routed) Assert.Equal("same-id", await f.WaitAsync(TimeSpan.FromSeconds(10)));
            foreach (var f in unroutable) await Assert.ThrowsAsync<UnroutableError>(() => f.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            await RealBroker.DeleteExchangeAsync(exchange);
            await RealBroker.DeleteQueueAsync(name);
        }
    }

    [Fact]
    public async Task ServicesComeBackWhenTheBrokerDropsTheConnection()
    {
        var c = await ContextAsync();
        await ServeAsync(c);
        var p = Proxy(c);
        Assert.Equal(2, (await p.AddAsync(new AddRequest { A = 1, B = 1 })).Result);
        var reconnected = 0;
        c.Connection.Reconnected += () => Interlocked.Increment(ref reconnected);
        Assert.True(await RealBroker.WaitForAsync(async () => await RealBroker.CloseConnectionsAsync() > 0, TimeSpan.FromSeconds(10)));
        Assert.True(await RealBroker.WaitForAsync(() => Task.FromResult(reconnected >= 1), TimeSpan.FromSeconds(20)));
        Assert.Equal(5, (await p.AddAsync(new AddRequest { A = 2, B = 3 })).Result);
    }

    [Fact]
    public async Task AChangedRetryDelayIsReportedPlainly()
    {
        var c = await ContextAsync();
        await (await ServeAsync(c, new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 1000) })).DisposeAsync();
        var other = await ContextAsync();
        var s = new Calc(other, new MessageServiceOptions { Retry = new RetryOptions(RetryDelayMs: 2000) }, name);
        var e = await Assert.ThrowsAsync<RetryQueueMismatchError>(s.InitAsync);
        Assert.Contains("retryDelayMs", e.Message);
    }

    [Fact]
    public async Task APriorityQueueServesHigherPrioritiesFirst()
    {
        var c = await ContextAsync();
        var s = await ServeAsync(c, new MessageServiceOptions { MaxPriority = 2 });
        await s.StopConsumingAsync();
        // Queue them up behind a stopped consumer, then let one go.
        var p = Proxy(c);
        var calls = Enumerable.Range(0, 3)
            .Select(i => p.WhoamiAsync(new Nothing(), new CallOptions { Actor = "low" + i, Priority = Config.PriorityNormal }))
            .ToList();
        calls.Add(p.WhoamiAsync(new Nothing(), new CallOptions { Actor = "high", Priority = Config.PriorityControl }));
        Assert.True(await RealBroker.WaitForAsync(async () => await RealBroker.QueueMessagesAsync(name) == 4, TimeSpan.FromSeconds(10)));
        var second = await ServeAsync(c, new MessageServiceOptions { MaxPriority = 2 });
        await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(10));
        var order = second.Order.ToList();
        Assert.Equal("high", order[0]);
        Assert.DoesNotContain("high", order.Skip(1));
    }

    [Fact]
    public async Task EventsAndEventRetryOverRabbitMq()
    {
        var c = await ContextAsync();
        var s = await ServeAsync(c, new MessageServiceOptions { EventRetry = new EventRetryOptions(1, 100) });
        var attempts = 0;
        var ok = new ConcurrentQueue<string>();
        var topic = "EVENT." + name;
        await s.SubscribeEventAsync<Ping>((e, _, _) =>
        {
            if (e.Id == "bad")
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("no");
            }
            ok.Enqueue(e.Id);
            return Task.CompletedTask;
        }, topic);
        await c.PublishEventAsync(new Ping { Id = "good", N = CustomTypes.Bigint(1) }, topic);
        await c.PublishEventAsync(new Ping { Id = "bad" }, topic);
        Assert.True(await RealBroker.WaitForAsync(() => Task.FromResult(ok.Count == 1), TimeSpan.FromSeconds(10)));
        Assert.True(await RealBroker.WaitForAsync(async () => await RealBroker.QueueMessagesAsync(name + ".Events.DLQ") == 1,
            TimeSpan.FromSeconds(10)));
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task CancellingAStreamReachesTheProducerAcrossTheBroker()
    {
        var c = await ContextAsync();
        var s = await ServeAsync(c);
        var p = Proxy(c);
        await foreach (var _ in p.Ticks(new TickRequest { Count = 1000, DelayMs = 20 })) break;
        Assert.True(await RealBroker.WaitForAsync(() => Task.FromResult(s.Stopped == 1), TimeSpan.FromSeconds(10)));
    }
}
