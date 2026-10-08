using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Protobus;

/// <summary>
/// One process's place on the bus. It owns the connection, the message factory and the two
/// dispatchers, and is what services and proxies are built on.
/// <code>
/// await using var ctx = new Context();
/// await ctx.InitAsync("amqp://guest:guest@localhost:5672/");
/// </code>
/// Disposing it fails pending calls and streams at once, closes the connection, and then waits up
/// to <see cref="Config.ShutdownDrainTimeoutMs"/> for handlers still running.
/// </summary>
public sealed class Context : IAsyncDisposable
{
    private readonly ContextOptions options;
    private readonly MessageDispatcher messageDispatcher;
    private readonly EventDispatcher eventDispatcher;
    private int closed;

    public Context(ContextOptions? options = null)
    {
        this.options = options ?? ContextOptions.Default;
        Connection = new Connection(this.options.Transport);
        messageDispatcher = new MessageDispatcher(Connection);
        eventDispatcher = new EventDispatcher(Connection);
        Connection.Reconnecting += (attempt, delay) =>
            Logger.Info($"Context: reconnecting (attempt {attempt}, delay {delay}ms)");
        Connection.Reconnected += () => Logger.Info("Context: reconnected successfully");
        Connection.Error += err => Logger.Error("Context: connection error - " + Errors.MessageOf(err));
    }

    /// <summary>Connect and start the dispatchers.</summary>
    public async Task InitAsync(string amqpUrl)
    {
        await Connection.ConnectAsync(amqpUrl, options.Reconnection).ConfigureAwait(false);
        await messageDispatcher.InitAsync().ConfigureAwait(false);
        await eventDispatcher.InitAsync().ConfigureAwait(false);
    }

    public bool IsConnected => Connection.IsConnected;

    public bool IsReconnecting => Connection.IsReconnecting;

    /// <summary>Publish an encoded RequestContainer and return the raw reply (null when <see cref="CallOptions.Rpc"/> is false).</summary>
    public Task<byte[]?> PublishMessageAsync(byte[] content, string routingKey, CallOptions? options = null,
        CancellationToken cancellationToken = default) =>
        messageDispatcher.PublishAsync(content, routingKey, options, cancellationToken);

    /// <summary>Publish an encoded RequestContainer expecting a streaming reply.</summary>
    public MessageDispatcher.ChunkStream PublishStreamingMessage(byte[] content, string routingKey,
        StreamOptions? options = null, CancellationToken cancellationToken = default) =>
        messageDispatcher.PublishStreaming(content, routingKey, options, cancellationToken);

    /// <summary>Publish an event of the message's own type, on <c>EVENT.&lt;type&gt;</c> unless a topic is given.</summary>
    public Task PublishEventAsync(IMessage content, string? topic = null, CancellationToken cancellationToken = default) =>
        eventDispatcher.PublishAsync(content.Descriptor.FullName, content, topic, cancellationToken);

    public Task PublishEventAsync(string type, IMessage content, string? topic,
        CancellationToken cancellationToken = default) =>
        eventDispatcher.PublishAsync(type, content, topic, cancellationToken);

    public MessageFactory Factory { get; } = new();

    internal MessageDispatcher MessageDispatcher => messageDispatcher;

    public Connection Connection { get; }

    /// <summary>Close the dispatchers and the connection, then wait for running handlers. Safe to call more than once.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) == 1) return;
        // Disconnect first: it is what fails pending calls and streams, through the dispatchers'
        // disconnect listeners, which closing them would detach.
        await Connection.DisconnectAsync().ConfigureAwait(false);
        try
        {
            await messageDispatcher.CloseAsync().ConfigureAwait(false);
            await eventDispatcher.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Logger.Debug("Context: closing the dispatchers: " + e.Message);
        }
        if (!await Connection.DrainInFlightAsync(Config.ShutdownDrainTimeoutMs).ConfigureAwait(false))
            Logger.Warn($"Context: {Connection.InFlightDeliveries} handler(s) still running after "
                + $"{Config.ShutdownDrainTimeoutMs}ms; closing anyway");
    }
}
