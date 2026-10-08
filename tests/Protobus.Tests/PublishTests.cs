using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Pbtest;
using Protobus.Amqp;
using Protobus.Testing;
using Xunit;

namespace Protobus.Tests;

public class PublishTests : MemoryBus
{
    private static readonly PublishOptions Plain = new(new MessageProperties());

    private async Task<IAmqpChannel> QueueChannel(string queue)
    {
        var ch = await Ctx.Connection.OpenChannelAsync();
        await Ctx.Connection.DeclareQueueAsync(ch, queue, true, false, false);
        return ch;
    }

    [Fact]
    public async Task AConfirmedPublishReturnsItsMessageId()
    {
        var ch = await QueueChannel("q");
        var id = await Ctx.Connection.PublishAsync(ch, "", "q", new byte[] { 1 }, Plain);
        Assert.Equal(36, id.Length);
        Assert.Equal(id, Broker.Peek("q")[0].Properties.MessageId);
    }

    [Fact]
    public async Task ANackIsADefiniteFailure()
    {
        var ch = await QueueChannel("q");
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Nack);
        await Assert.ThrowsAsync<PublishNackedError>(() => Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), Plain));
    }

    [Fact]
    public async Task AMissingConfirmIsAnAmbiguousTimeout()
    {
        Config.Set("PUBLISH_CONFIRM_TIMEOUT_MS", "100");
        var ch = await QueueChannel("q");
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var e = await Assert.ThrowsAsync<PublishConfirmTimeoutError>(() => Ctx.Connection.PublishAsync(ch, "", "q",
            Array.Empty<byte>(), new PublishOptions(new MessageProperties { MessageId = "m1" })));
        Assert.Equal("m1", e.MessageId);
        // The broker stored it all the same: the outcome was unknown, not failed.
        Assert.Equal(1, Broker.QueueDepth("q"));
    }

    [Fact]
    public async Task AChannelClosingUnderAPendingConfirmIsAmbiguous()
    {
        var ch = await QueueChannel("q");
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var f = Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), Plain);
        Assert.True(await Eventually(() => Broker.HeldConfirms == 1));
        await ch.CloseAsync();
        await Assert.ThrowsAsync<ChannelClosedError>(() => f);
    }

    [Fact]
    public async Task UnconfirmedPublishesAreBoundedPerChannel()
    {
        Config.Set("MAX_OUTSTANDING_CONFIRMS", "2");
        var ch = await QueueChannel("q");
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var fs = Enumerable.Range(0, 5).Select(_ => Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), Plain)).ToList();
        // Writes reach the broker on the channel's writer, a moment after the call.
        Assert.True(await Eventually(() => Broker.HeldConfirms == 2));
        await Broker.FlushAsync();
        await Task.Delay(20);
        Assert.Equal(2, Broker.HeldConfirms);
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Ack);
        Broker.ReleaseHeldConfirms(ConfirmOutcome.Ack);
        await Task.WhenAll(fs);
        Assert.Equal(5, Broker.QueueDepth("q"));
    }

    [Fact]
    public async Task AMandatoryPublishToNothingIsUnroutable()
    {
        var ch = await Ctx.Connection.OpenChannelAsync();
        await Ctx.Connection.DeclareExchangeAsync(ch, "x.topic", "topic");
        await Assert.ThrowsAsync<UnroutableError>(() => Ctx.Connection.PublishAsync(ch, "x.topic", "nowhere",
            Array.Empty<byte>(), new PublishOptions(new MessageProperties(), true)));
        // Not mandatory: dropped by the broker, and fine.
        await Ctx.Connection.PublishAsync(ch, "x.topic", "nowhere", Array.Empty<byte>(), Plain);
    }

    [Fact]
    public async Task ReturnsAreToldApartWhenPublishesShareAMessageId()
    {
        var ch = await QueueChannel("q");
        await Ctx.Connection.DeclareExchangeAsync(ch, "x.topic", "topic");
        await ch.BindQueueAsync("q", "x.topic", "routed");
        var props = new MessageProperties { MessageId = "same" };
        Broker.SetConfirmMode(MemoryBroker.ConfirmMode.Drop);
        var routed = Ctx.Connection.PublishAsync(ch, "x.topic", "routed", Array.Empty<byte>(), new PublishOptions(props, true));
        var unroutable = Ctx.Connection.PublishAsync(ch, "x.topic", "nowhere", Array.Empty<byte>(), new PublishOptions(props, true));
        Assert.True(await Eventually(() => Broker.HeldConfirms == 2));
        Broker.ReleaseHeldConfirms(ConfirmOutcome.Ack);
        Assert.Equal("same", await routed);
        // The memory broker reports returns directly; the tag is what RabbitMQ needs.
        Assert.Equal("same", await unroutable);
        var headers = Broker.Peek("q")[0].Properties.Headers;
        Assert.True(headers == null || !headers.ContainsKey("x-protobus-publish-tag"));
    }

    [Fact]
    public async Task APublishOnAClosedChannelFailsAtOnce()
    {
        var ch = await QueueChannel("q");
        await ch.CloseAsync();
        await Assert.ThrowsAsync<ChannelClosedError>(() => Ctx.Connection.PublishAsync(ch, "", "q", Array.Empty<byte>(), Plain));
    }

    [Fact]
    public async Task RetryAndDeadLetterCopiesKeepTheMessagesProperties()
    {
        var s = await ServeAsync(new MessageServiceOptions { Retry = new RetryOptions(MaxRetries: 1, RetryDelayMs: 10) });
        await Assert.ThrowsAsync<RemoteError>(() => Proxy().FailAsync(new FailRequest { Id = "p" }, new CallOptions { Priority = 1 }));
        Assert.True(await Eventually(() => Broker.QueueDepth("pbtest.Calc.DLQ") == 1));
        var dead = Broker.Peek("pbtest.Calc.DLQ")[0].Properties;
        Assert.Equal((byte)1, dead.Priority);
        Assert.Equal("application/octet-stream", dead.ContentType);
        Assert.Equal((byte)2, dead.DeliveryMode);
        Assert.Null(dead.ReplyTo);
        Assert.Equal(2, s.FailAttempts);
    }
}
