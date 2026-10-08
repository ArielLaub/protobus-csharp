using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Protobus.Amqp;

namespace Protobus;

/// <summary>Publishes events to the events exchange.</summary>
public sealed class EventDispatcher
{
    private readonly Connection connection;
    private readonly SemaphoreSlim channelLock = new(1, 1);
    private volatile IAmqpChannel? channel;
    private volatile bool initialized;
    private readonly IDisposable detachRestorer;

    public EventDispatcher(Connection connection)
    {
        this.connection = connection;
        connection.Disconnected += OnDisconnected;
        detachRestorer = connection.RegisterRestorer(async _ =>
        {
            if (!initialized) return;
            Logger.Info("EventDispatcher: reconnected, re-initializing channel");
            await OpenAsync().ConfigureAwait(false);
        });
    }

    public bool IsInitialized => initialized;

    private void OnDisconnected() => channel = null;

    public async Task InitAsync()
    {
        if (initialized) return;
        await OpenAsync().ConfigureAwait(false);
        initialized = true;
    }

    private async Task<IAmqpChannel> OpenAsync()
    {
        var ch = await connection.OpenChannelAsync().ConfigureAwait(false);
        // Declared by the publisher too, so an event published before any subscriber exists is
        // not refused for want of an exchange.
        await connection.DeclareExchangeAsync(ch, Config.EventsExchangeName, "topic").ConfigureAwait(false);
        channel = ch;
        return ch;
    }

    private async Task<IAmqpChannel?> PublishChannelAsync()
    {
        var ch = channel;
        if (ch is { IsOpen: true }) return ch;
        await channelLock.WaitAsync().ConfigureAwait(false);
        try
        {
            ch = channel;
            if (ch is { IsOpen: true } || !connection.IsReady) return ch;
            Logger.Warn("EventDispatcher: publishing channel lost on a live connection; reopening it");
            return await OpenAsync().ConfigureAwait(false);
        }
        finally
        {
            channelLock.Release();
        }
    }

    /// <summary>
    /// Publish an event. <paramref name="type"/> is the payload's full message name; the topic
    /// defaults to <c>EVENT.&lt;type&gt;</c>. Completes once the broker has confirmed it. Not
    /// mandatory: an event nobody subscribes to is normal.
    /// </summary>
    public async Task PublishAsync(string type, IMessage content, string? topic = null,
        CancellationToken cancellationToken = default)
    {
        if (!connection.IsConnected && !connection.IsReconnecting) throw new NotConnectedError();
        var t = string.IsNullOrEmpty(topic) ? "EVENT." + type : topic;
        byte[] @event;
        try
        {
            @event = MessageFactory.BuildEvent(type, content, t);
        }
        catch (Exception e) when (e is not ProtobusException)
        {
            // No payload in the line: events carry personal data too.
            Logger.Error($"failed building event '{type}': {e.Message}");
            throw new InvalidMessageError($"failed building event '{type}': {e.Message}");
        }
        var props = new MessageProperties
        {
            CorrelationId = Guid.NewGuid().ToString(),
            ContentType = "application/octet-stream",
            DeliveryMode = 2,
        };
        await connection.WhenReadyAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        var ch = await PublishChannelAsync().ConfigureAwait(false);
        await connection.PublishAsync(ch, Config.EventsExchangeName, t, @event, new PublishOptions(props))
            .WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task CloseAsync()
    {
        connection.Disconnected -= OnDisconnected;
        detachRestorer.Dispose();
        var ch = channel;
        if (ch is { IsOpen: true }) await ch.CloseAsync().ConfigureAwait(false);
    }
}
