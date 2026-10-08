using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Protobus.Amqp;
using Protobus.Internal;

namespace Protobus;

/// <summary>
/// The caller's side of RPC: publishes requests and routes their replies, unary and streaming,
/// back to the call that is waiting for them.
/// </summary>
public sealed class MessageDispatcher
{
    /// <summary>AMQP carries <c>message-id</c> as a shortstr: one length byte, so 255 at most.</summary>
    internal const int MaxMessageIdBytes = 255;

    private readonly Connection connection;
    private readonly CallbackListener callbackListener;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> callbacks = new();
    /// <summary>
    /// correlationId → in-flight streaming reply. Distinct from <c>callbacks</c>, so a streaming
    /// reply cannot resolve an unrelated unary call.
    /// </summary>
    private readonly ConcurrentDictionary<string, StreamCall> pendingStreams = new();
    /// <summary>Bytes buffered across every pending stream: a bound on the process, not one call.</summary>
    private long totalBufferedBytes;
    private readonly SemaphoreSlim channelLock = new(1, 1);
    private volatile IAmqpChannel? channel;
    private volatile bool initialized;
    private readonly IDisposable detachRestorer;

    public MessageDispatcher(Connection connection)
    {
        this.connection = connection;
        callbackListener = new CallbackListener(connection);
        connection.Disconnected += OnDisconnected;
        detachRestorer = connection.RegisterRestorer(_ => RestoreAsync());
    }

    public bool IsInitialized => initialized;

    public async Task InitAsync()
    {
        if (initialized) return;
        await OpenPublishChannelAsync().ConfigureAwait(false);
        await callbackListener.InitAsync(OnResult, null).ConfigureAwait(false);
        // A reply queue rebuilt on a live connection is a new, server-named queue: requests in
        // flight name the old one, so their replies will never come.
        callbackListener.OnQueueReplaced(() =>
            FailPending(new DisconnectedError("the reply queue was replaced while the call was pending")));
        await callbackListener.StartAsync().ConfigureAwait(false);
        initialized = true;
    }

    /// <summary>
    /// Open the publishing channel and declare the exchanges this dispatcher publishes to, so a
    /// client works against a broker no service has touched yet.
    /// </summary>
    private async Task<IAmqpChannel> OpenPublishChannelAsync()
    {
        var ch = await connection.OpenChannelAsync().ConfigureAwait(false);
        await connection.DeclareExchangeAsync(ch, Config.BusExchangeName, "topic").ConfigureAwait(false);
        await connection.DeclareExchangeAsync(ch, Config.CancelExchangeName, "fanout").ConfigureAwait(false);
        channel = ch;
        return ch;
    }

    /// <summary>
    /// The publishing channel, reopened when it was lost on a live connection (a channel error
    /// closes only that channel).
    /// </summary>
    private async Task<IAmqpChannel?> PublishChannelAsync()
    {
        var ch = channel;
        if (ch is { IsOpen: true }) return ch;
        await channelLock.WaitAsync().ConfigureAwait(false);
        try
        {
            ch = channel;
            if (ch is { IsOpen: true } || !connection.IsReady) return ch;
            Logger.Warn("MessageDispatcher: publishing channel lost on a live connection; reopening it");
            return await OpenPublishChannelAsync().ConfigureAwait(false);
        }
        finally
        {
            channelLock.Release();
        }
    }

    private async Task RestoreAsync()
    {
        if (!initialized) return;
        Logger.Info("MessageDispatcher: reconnected, re-initializing channel");
        await OpenPublishChannelAsync().ConfigureAwait(false);
        Logger.Info("MessageDispatcher: successfully re-initialized after reconnection");
    }

    /// <summary>Fail every pending call and stream: their replies went with the connection.</summary>
    private void OnDisconnected()
    {
        Logger.Debug("MessageDispatcher: connection lost, rejecting pending callbacks");
        channel = null;
        FailPending(new DisconnectedError());
    }

    private void FailPending(DisconnectedError error)
    {
        foreach (var e in callbacks)
            if (callbacks.TryRemove(e)) e.Value.TrySetException(error);
        foreach (var s in pendingStreams.Values) s.Fail(error);
    }

    /// <summary>A reply, delivered in order on the reply queue.</summary>
    private Task<HandlerResult> OnResult(byte[] content, string id, MessageHandlerContext context)
    {
        if (pendingStreams.TryGetValue(id, out var stream))
        {
            stream.OnChunk(content, context.Headers);
            return Task.FromResult(HandlerResult.None);
        }
        // The deadline stays armed: it belongs to the caller's result, which may still be
        // waiting for the request's confirm.
        if (callbacks.TryRemove(id, out var pending)) pending.TrySetResult(content);
        return Task.FromResult(HandlerResult.None);
    }

    /// <summary>
    /// A caller-supplied messageId, or null to have one minted. Blank is refused rather than
    /// treated as absent: an id derived from a field that came out empty would give every attempt
    /// a different identity, and no deduplication at all.
    /// </summary>
    internal static string? ValidateMessageId(string? messageId)
    {
        if (messageId == null) return null;
        if (messageId.Trim().Length == 0)
            throw new InvalidMessageIdError($"messageId must be a non-empty string, got \"{messageId}\". "
                + "Leave it unset to have one generated.");
        var bytes = Encoding.UTF8.GetByteCount(messageId);
        if (bytes > MaxMessageIdBytes)
            throw new InvalidMessageIdError($"messageId is {bytes} bytes; AMQP carries message-id as a shortstr, so it "
                + $"must be at most {MaxMessageIdBytes}. Hash a long key rather than concatenating it.");
        return messageId;
    }

    /// <summary>
    /// Publish a request and wait for its reply (or, with <see cref="CallOptions.Rpc"/> false, for
    /// the broker's confirm). The deadline, <see cref="CallOptions.TimeoutMs"/> or
    /// <see cref="Config.RpcCallTimeoutMs"/>, starts once the connection is ready and bounds the
    /// confirm as well as the reply. When the publish fails, its error wins over the deadline:
    /// "the request never left" is the more specific answer.
    /// </summary>
    /// <param name="cancellationToken">Stops waiting; the request may already have been delivered.</param>
    /// <returns>the raw reply, or null when <see cref="CallOptions.Rpc"/> is false</returns>
    public async Task<byte[]?> PublishAsync(byte[] content, string routingKey, CallOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var o = options ?? CallOptions.Default;
        var priority = Priority.ValidatePriority(o.Priority);
        var callerMessageId = ValidateMessageId(o.MessageId);
        // A reconnection is waited through; anything else with no connection is a caller error,
        // reported at once.
        if (!connection.IsConnected && !connection.IsReconnecting) throw new NotConnectedError();
        await connection.WhenReadyAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        return await SendAsync(content, routingKey, o, priority, callerMessageId, cancellationToken).ConfigureAwait(false);
    }

    private async Task<byte[]?> SendAsync(byte[] content, string routingKey, CallOptions o, int? priority,
        string? callerMessageId, CancellationToken cancellationToken)
    {
        var rpc = o.Rpc;
        var id = Guid.NewGuid().ToString();
        var props = new MessageProperties
        {
            ContentType = "application/octet-stream",
            CorrelationId = id,
            DeliveryMode = 2,
            ReplyTo = rpc ? callbackListener.QueueName : null,
            // Set only when asked for, so a publish with no priority carries none.
            Priority = priority is { } p ? (byte)p : null,
            MessageId = callerMessageId,
        };
        var ch = await PublishChannelAsync().ConfigureAwait(false);
        // An RPC request that routes nowhere means no service is bound to the key: worth learning
        // now, as UnroutableError, rather than after the timeout. Not for one-way publishes, where
        // no consumer may be normal.
        var publish = new PublishOptions(props, rpc);
        if (!rpc)
        {
            await connection.PublishAsync(ch, Config.BusExchangeName, routingKey, content, publish)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            return null;
        }

        var limit = o.TimeoutMs ?? Config.RpcCallTimeoutMs;
        // Armed BEFORE publishing: a fast service can reply while the confirm is still in flight.
        var pending = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        callbacks[id] = pending;
        // The deadline is the caller's: it ends the call whatever is still outstanding, the reply
        // or the request's own confirm.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMilliseconds(limit));
        var ended = Task.Delay(Timeout.Infinite, deadline.Token);
        try
        {
            var published = connection.PublishAsync(ch, Config.BusExchangeName, routingKey, content, publish);
            // Observed here whatever the outcome, so a late failure is never unobserved.
            _ = published.ContinueWith(t => _ = t.Exception, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            var first = await Task.WhenAny(published, pending.Task, ended).ConfigureAwait(false);
            // A timeout or a disconnect settles the call at once, without waiting for the confirm.
            // A reply, though, is only taken once the request is confirmed, because a failed
            // publish is the more specific answer.
            if (first == pending.Task && pending.Task.IsCompletedSuccessfully)
                first = await Task.WhenAny(published, ended).ConfigureAwait(false);
            if (first == published)
            {
                // The request never made it: no reply is coming.
                await published.ConfigureAwait(false);
                first = await Task.WhenAny(pending.Task, ended).ConfigureAwait(false);
            }
            if (first == ended)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new RpcTimeoutError($"no reply for {routingKey} (correlationId {id}) within {limit}ms");
            }
            return await pending.Task.ConfigureAwait(false);
        }
        finally
        {
            callbacks.TryRemove(new KeyValuePair<string, TaskCompletionSource<byte[]>>(id, pending));
        }
    }

    /// <summary>
    /// Publish a request expecting a streaming reply. Returns at once; the request is published as
    /// soon as the connection is ready, and a failure to publish surfaces from the stream's first
    /// read.
    ///
    /// Cancelling <paramref name="cancellationToken"/>, or leaving the stream before its end,
    /// releases its buffer and tells the producer to stop; the stream then ends without an error.
    /// That notice is best effort and cooperative: a producer that ignores its token runs to
    /// completion, and its chunks are dropped here.
    /// </summary>
    public ChunkStream PublishStreaming(byte[] content, string routingKey, StreamOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var o = options ?? StreamOptions.Default;
        if (!connection.IsConnected && !connection.IsReconnecting) throw new NotConnectedError();
        var id = Guid.NewGuid().ToString();
        var stream = new StreamCall(this, id, o.IdleTimeoutMs ?? Config.StreamIdleTimeoutMs);
        if (cancellationToken.IsCancellationRequested)
        {
            // Cancelled before it began: nothing to send and nothing to wait for.
            stream.EndQuietly();
            return new ChunkStream(stream);
        }
        pendingStreams[id] = stream;
        if (cancellationToken.CanBeCanceled)
        {
            var registration = cancellationToken.Register(() => stream.Cancel(false));
            stream.ReleaseSignal = () => registration.Dispose();
        }
        stream.ArmIdle();
        var props = new MessageProperties
        {
            ContentType = "application/octet-stream",
            CorrelationId = id,
            ReplyTo = callbackListener.QueueName,
            DeliveryMode = 2,
        };
        Connection.Background(async () =>
        {
            try
            {
                await connection.WhenReadyAsync().ConfigureAwait(false);
                // The channel is read after readiness: a reconnection replaces it.
                var ch = await PublishChannelAsync().ConfigureAwait(false);
                await connection.PublishAsync(ch, Config.BusExchangeName, routingKey, content, new PublishOptions(props))
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                stream.Fail(e);
            }
        });
        return new ChunkStream(stream);
    }

    internal int PendingStreamCount => pendingStreams.Count;

    public async Task CloseAsync()
    {
        connection.Disconnected -= OnDisconnected;
        detachRestorer.Dispose();
        if (callbackListener.IsInitialized) await callbackListener.CloseAsync().ConfigureAwait(false);
        var ch = channel;
        if (ch is { IsOpen: true }) await ch.CloseAsync().ConfigureAwait(false);
    }

    // ---- streams -----------------------------------------------------------------------

    /// <summary>Read x-protobus-seq; absent or unparseable disables validation rather than inventing a violation.</summary>
    internal static long? ParseSeq(IDictionary<string, object?>? headers) =>
        Headers.Integer(Headers.Get(headers, Config.HeaderSeq)) is { } n && n >= 0 ? n : null;

    internal static bool ParseFinal(IDictionary<string, object?>? headers) =>
        Headers.Bool(Headers.Get(headers, Config.HeaderFinal));

    /// <summary>One pending streaming call: chunks buffered until the caller takes them.</summary>
    internal sealed class StreamCall
    {
        private readonly MessageDispatcher owner;
        internal readonly string Id;
        private readonly long idleMs;
        private readonly object sync = new();
        private readonly Queue<byte[]> chunks = new();
        private long bufferedBytes;
        /// <summary>Highest sequence accepted, or null before the first and for peers that send none.</summary>
        private long? lastSeq;
        private bool ended;
        private Exception? error;
        private bool cancelled;
        private IDisposable? idle;
        private TaskCompletionSource? waiter;
        internal Action ReleaseSignal = () => { };

        internal StreamCall(MessageDispatcher owner, string id, long idleMs)
        {
            this.owner = owner;
            Id = id;
            this.idleMs = idleMs;
        }

        /// <summary>Wake the reader; called holding <c>sync</c>.</summary>
        private void Wake()
        {
            var w = waiter;
            waiter = null;
            w?.TrySetResult();
        }

        /// <summary>
        /// Restart the idle deadline, on progress from either side: a chunk arriving, or a chunk
        /// handed to the caller. Armed at call time, so a caller that never reads still releases
        /// what the call holds.
        /// </summary>
        internal void ArmIdle()
        {
            lock (sync)
            {
                idle?.Dispose();
                if (ended) return;
                idle = owner.connection.Schedule(TimeSpan.FromMilliseconds(idleMs), OnIdle);
            }
        }

        private void OnIdle()
        {
            lock (sync)
            {
                if (ended) return;
                error = new StreamTimeoutError($"No streaming chunk received within {idleMs}ms");
                ended = true;
                Wake();
            }
            // The producer is still working for a caller that stopped listening.
            Cancel(true);
        }

        internal void EndQuietly()
        {
            lock (sync)
            {
                ended = true;
                Wake();
            }
        }

        internal void Fail(Exception err)
        {
            bool completed;
            lock (sync)
            {
                // A stream that already ended without an error (its final chunk arrived, or its
                // caller cancelled it) keeps that outcome: a late publish failure or a disconnect
                // cannot turn it into an error.
                completed = ended && error == null;
                if (!completed)
                {
                    error ??= err;
                    ended = true;
                }
                idle?.Dispose();
                Wake();
            }
            if (completed)
            {
                // No more replies are coming for it; only the registration goes.
                ReleaseSignal();
                owner.pendingStreams.TryRemove(new KeyValuePair<string, StreamCall>(Id, this));
                return;
            }
            // Released now, keeping the error for the caller: one that never reads the stream
            // again must not hold its entry or its cancellation registration.
            Release();
        }

        private void ReleaseBuffer()
        {
            long bytes;
            lock (sync)
            {
                bytes = bufferedBytes;
                bufferedBytes = 0;
                chunks.Clear();
            }
            if (bytes > 0) owner.SubtractBuffered(bytes);
        }

        /// <summary>Everything the call holds, released on any terminal outcome.</summary>
        internal void Release()
        {
            lock (sync) idle?.Dispose();
            ReleaseSignal();
            owner.pendingStreams.TryRemove(new KeyValuePair<string, StreamCall>(Id, this));
            ReleaseBuffer();
        }

        /// <summary>
        /// Stop the producer and release the call. <paramref name="notifyOnly"/> is for the failure
        /// paths, which have already recorded the error the caller is about to see.
        /// </summary>
        internal void Cancel(bool notifyOnly)
        {
            lock (sync)
            {
                if (cancelled) return;
                cancelled = true;
                if (!notifyOnly)
                {
                    ended = true;
                    Wake();
                }
            }
            Release();
            Logger.Debug("cancelling stream " + Id);
            var props = new MessageProperties { CorrelationId = Id, ContentType = "application/octet-stream" };
            // Fire and forget: the caller has stopped waiting.
            Connection.Background(async () =>
            {
                try
                {
                    var ch = await owner.PublishChannelAsync().ConfigureAwait(false);
                    await owner.connection.PublishAsync(ch, Config.CancelExchangeName, "", Array.Empty<byte>(),
                        new PublishOptions(props)).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Debug($"failed to publish cancel for stream {Id}: {e.Message}");
                }
            });
        }

        /// <summary>A chunk, delivered in order on the reply queue.</summary>
        internal void OnChunk(byte[] body, IDictionary<string, object?>? headers)
        {
            var isFinal = ParseFinal(headers);
            var seq = ParseSeq(headers);
            var overflow = false;
            lock (sync)
            {
                if (ended) return;
                if (seq != null)
                {
                    var expected = lastSeq == null ? 0 : lastSeq.Value + 1;
                    if (seq < expected)
                    {
                        // Already seen: a redelivery, not new data.
                        Logger.Debug($"stream {Id}: dropping duplicate chunk seq={seq}");
                        if (isFinal)
                        {
                            ended = true;
                            Wake();
                        }
                        return;
                    }
                    if (seq > expected)
                    {
                        error = new StreamSequenceError($"stream {Id} lost at least one chunk: got seq={seq}, expected {expected}");
                        ended = true;
                        Wake();
                        overflow = true;
                    }
                    else
                    {
                        lastSeq = seq;
                    }
                }
                if (!overflow && body.Length > 0)
                {
                    var maxChunks = Config.StreamMaxBufferedChunks;
                    var maxBytes = Config.StreamMaxBufferedBytes;
                    var maxTotal = Config.StreamMaxTotalBufferedBytes;
                    var wouldBe = bufferedBytes + body.Length;
                    var wouldBeTotal = Interlocked.Read(ref owner.totalBufferedBytes) + body.Length;
                    if (chunks.Count + 1 > maxChunks || wouldBe > maxBytes || wouldBeTotal > maxTotal)
                    {
                        error = new StreamBackpressureError($"stream {Id} exceeded a buffer limit ({chunks.Count + 1} chunks / "
                            + $"{wouldBe} bytes for this call, {wouldBeTotal} bytes across all calls; limits are {maxChunks} "
                            + $"chunks / {maxBytes} bytes / {maxTotal} bytes total): the consumer is not keeping up with the "
                            + "producer");
                        ended = true;
                        Wake();
                        overflow = true;
                    }
                    else
                    {
                        chunks.Enqueue(body);
                        bufferedBytes = wouldBe;
                        Interlocked.Add(ref owner.totalBufferedBytes, body.Length);
                    }
                }
                if (!overflow)
                {
                    if (isFinal) ended = true;
                    Wake();
                }
            }
            if (overflow)
            {
                ReleaseBuffer();
                // The producer would keep going for a stream that has failed.
                Cancel(true);
            }
            else if (!isFinal)
            {
                ArmIdle();
            }
        }

        /// <summary>
        /// The next chunk, or null at the end; throws the stream's failure. A failure to publish
        /// the request is one of those outcomes, so a reader waits on all of them at once: an idle
        /// timeout or a cancel wakes it while the request's confirm is still outstanding.
        /// </summary>
        internal async ValueTask<byte[]?> NextAsync()
        {
            while (true)
            {
                Exception? failure = null;
                byte[]? chunk = null;
                var done = false;
                Task wait;
                lock (sync)
                {
                    if (error != null)
                    {
                        failure = error;
                        done = true;
                    }
                    else if (chunks.TryDequeue(out chunk))
                    {
                        bufferedBytes -= chunk.Length;
                        owner.SubtractBuffered(chunk.Length);
                    }
                    else if (ended)
                    {
                        done = true;
                    }
                    waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    wait = waiter.Task;
                }
                if (chunk != null)
                {
                    if (!Ended) ArmIdle();
                    return chunk;
                }
                if (done)
                {
                    Release();
                    if (failure != null) throw failure;
                    return null;
                }
                await wait.ConfigureAwait(false);
            }
        }

        internal bool Ended { get { lock (sync) return ended; } }

        internal bool Finished { get { lock (sync) return ended && chunks.Count == 0; } }
    }

    private void SubtractBuffered(long bytes)
    {
        long current, next;
        do
        {
            current = Interlocked.Read(ref totalBufferedBytes);
            next = Math.Max(0, current - bytes);
        } while (Interlocked.CompareExchange(ref totalBufferedBytes, next, current) != current);
    }

    /// <summary>
    /// The raw reply bodies of one streaming call. Enumerate it with <c>await foreach</c>; leaving
    /// the loop before the end cancels the call. Enumerable once.
    /// </summary>
    public sealed class ChunkStream : IAsyncEnumerable<byte[]>, IAsyncDisposable
    {
        private readonly StreamCall call;

        internal ChunkStream(StreamCall call) => this.call = call;

        /// <summary>The next chunk, or null once the stream has ended.</summary>
        public ValueTask<byte[]?> NextAsync() => call.NextAsync();

        /// <summary>Cancel: the producer's token fires and its chunks stop being published. Idempotent.</summary>
        public void Cancel()
        {
            if (!call.Finished) call.Cancel(false);
            else call.Release();
        }

        public bool Finished => call.Finished;

        /// <summary>Whether the stream's final chunk (or its failure) has arrived, read or not.</summary>
        internal bool Ended => call.Ended;

        public async IAsyncEnumerator<byte[]> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            using var registration = cancellationToken.Register(Cancel);
            try
            {
                while (await call.NextAsync().ConfigureAwait(false) is { } chunk) yield return chunk;
            }
            finally
            {
                Cancel();
            }
        }

        public ValueTask DisposeAsync()
        {
            Cancel();
            return ValueTask.CompletedTask;
        }
    }
}
