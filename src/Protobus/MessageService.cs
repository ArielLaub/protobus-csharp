using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Protobus.Internal;

namespace Protobus;

/// <summary>
/// The base class every RPC service extends.
///
/// A service is named on the bus by <see cref="ServiceName"/>, normally its .proto
/// <c>&lt;package&gt;.&lt;Service&gt;</c>. It owns one durable queue of that name, bound to
/// <c>REQUEST.&lt;ServiceName&gt;.*</c>, which every replica consumes from, so the broker balances
/// load and fails over. It also owns <c>&lt;ServiceName&gt;.Events</c> for the events it
/// subscribes to.
///
/// The protobus code generator derives a <c>&lt;Service&gt;Protobus.Base</c> class from this one
/// with a method per rpc; implement those. Without codegen, extend this class directly, return the
/// schema from <see cref="Schema"/> (or register it with the context's factory), and register
/// handlers with <see cref="RegisterUnary{TRequest,TResponse}"/> or, over raw payload bytes,
/// <see cref="RegisterMethod"/>.
///
/// A handler's exception decides what happens to the request. A <see cref="HandledError"/> is an
/// answer: it reaches the caller with its code and is never retried. Anything else, or a
/// processing timeout, is a failure: the request is retried through the service's retry queue and
/// dead-lettered once retries are spent, and the caller is answered then.
/// </summary>
public abstract class MessageService : IAsyncDisposable
{
    private sealed class MethodEntry
    {
        internal bool Streaming;
        /// <summary>Decodes the payload; throws on bytes that are not the request type.</summary>
        internal Func<byte[], object> Decode = null!;
        /// <summary>Returns an <see cref="IMessage"/>, or raw payload bytes.</summary>
        internal Func<object, CallContext, Task<object>>? Unary;
        /// <summary>Yields <see cref="IMessage"/>s, or raw payload bytes.</summary>
        internal Func<object, CallContext, IAsyncEnumerable<object>>? Stream;
    }

    protected readonly Context Context;
    private readonly MessageListener listener;
    private readonly EventListener eventListener;
    private readonly CancelListener cancelListener;

    private readonly object sync = new();
    private readonly Dictionary<string, MethodEntry> methods = new();
    /// <summary>
    /// The service as its .proto declares it, which is not always ServiceName: instances sharing
    /// one schema are addressed under distinct runtime names (<c>Combat.Player.player6</c> serving
    /// the contract <c>Combat.Player</c>).
    /// </summary>
    private volatile string? contractServiceName;
    private volatile HashSet<string> declaredMethods = new();

    protected MessageService(Context context, MessageServiceOptions? options = null)
    {
        Context = context;
        Options = options ?? MessageServiceOptions.Default;
        listener = new MessageListener(context.Connection, Options.LateAck, Options.MaxConcurrent, Options.Retry,
            Options.ProcessingTimeoutMs, Options.MaxPriority);
        listener.SetBuildErrorReply(BuildTimeoutReply);
        eventListener = new EventListener(context.Connection, Options.EventRetry);
        cancelListener = new CancelListener(context.Connection);
    }

    /// <summary>
    /// The service's name on the bus. Several instances can share one schema under distinct names:
    /// the contract is found by trimming trailing segments until one names a known service.
    /// </summary>
    public abstract string ServiceName { get; }

    /// <summary>The compiled schema declaring the service, registered on init. Generated bases return theirs.</summary>
    protected virtual FileDescriptor? Schema => null;

    public MessageServiceOptions Options { get; }

    /// <summary>The contract this service serves, once <see cref="InitAsync"/> has resolved it.</summary>
    public string? ContractServiceName => contractServiceName;

    // ---- registration ------------------------------------------------------------------

    /// <summary>Register a typed unary handler, as generated code does.</summary>
    protected void RegisterUnary<TRequest, TResponse>(string name, MessageParser<TRequest> parser,
        Func<TRequest, CallContext, Task<TResponse>> handler)
        where TRequest : IMessage<TRequest> where TResponse : class, IMessage<TResponse>
    {
        Add(name, new MethodEntry
        {
            Decode = TypedDecoder(parser),
            Unary = async (request, call) => await handler((TRequest)request, call).ConfigureAwait(false),
        });
    }

    /// <summary>Register a typed server-streaming handler, as generated code does.</summary>
    protected void RegisterStream<TRequest, TResponse>(string name, MessageParser<TRequest> parser,
        Func<TRequest, CallContext, IAsyncEnumerable<TResponse>> handler)
        where TRequest : IMessage<TRequest> where TResponse : class, IMessage<TResponse>
    {
        Add(name, new MethodEntry
        {
            Streaming = true,
            Decode = TypedDecoder(parser),
            Stream = (request, call) => handler((TRequest)request, call),
        });
    }

    /// <summary>
    /// Register a unary handler over raw payloads: it receives the request's serialized bytes and
    /// returns the response's. Google.Protobuf for C# has no dynamic messages, so this is the
    /// untyped form; nothing checks the bytes it returns.
    /// </summary>
    protected void RegisterMethod(string name, Func<byte[], CallContext, Task<byte[]>> handler)
    {
        Add(name, new MethodEntry
        {
            Decode = payload => payload,
            Unary = async (request, call) => await handler((byte[])request, call).ConfigureAwait(false),
        });
    }

    /// <summary>A server-streaming handler over raw payloads; see <see cref="RegisterMethod"/>.</summary>
    protected void RegisterStreamingMethod(string name, Func<byte[], CallContext, IAsyncEnumerable<byte[]>> handler)
    {
        Add(name, new MethodEntry
        {
            Streaming = true,
            Decode = payload => payload,
            Stream = (request, call) => handler((byte[])request, call),
        });
    }

    private static Func<byte[], object> TypedDecoder<T>(MessageParser<T> parser) where T : IMessage<T> =>
        payload =>
        {
            var m = parser.ParseFrom(payload);
            CustomTypes.Validate(m);
            return m;
        };

    private void Add(string name, MethodEntry entry)
    {
        lock (sync) methods[name] = entry;
    }

    // ---- lifecycle ---------------------------------------------------------------------

    private bool TryResolveContract()
    {
        if (contractServiceName != null) return true;
        var factory = Context.Factory;
        var candidate = ServiceName;
        while (true)
        {
            if (factory.HasService(candidate))
            {
                declaredMethods = new HashSet<string>(factory.ServiceMethodNames(candidate));
                contractServiceName = candidate;
                return true;
            }
            var cut = candidate.LastIndexOf('.');
            if (cut <= 0) return false;
            candidate = candidate.Substring(0, cut);
        }
    }

    private void ResolveContract()
    {
        if (!TryResolveContract())
            throw new MissingProtoError($"no service in the schema matches '{ServiceName}' or any prefix of it; "
                + "the schema must declare the service this class serves");
    }

    /// <summary>Register the schema if needed, declare the queues and start consuming.</summary>
    public virtual async Task InitAsync()
    {
        try
        {
            if (Schema is { } schema) Context.Factory.Register(schema);
            ResolveContract();
            await listener.InitAsync(OnMessage, ServiceName).ConfigureAwait(false);
            await eventListener.InitAsync(null, ServiceName + ".Events").ConfigureAwait(false);
            await listener.SubscribeAsync("REQUEST." + ServiceName + ".*").ConfigureAwait(false);
            await listener.StartAsync().ConfigureAwait(false);
            await eventListener.StartAsync().ConfigureAwait(false);
            // Started last: it only matters once requests can arrive.
            await cancelListener.StartAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Logger.Error($"error initializing service {ServiceName} - {e}");
            await CloseQuietlyAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Stop accepting new requests and events, leaving channels open so work in hand can finish:
    /// the first step of a graceful shutdown. Pair it with
    /// <see cref="Protobus.Connection.DrainInFlightAsync"/> before closing anything.
    /// </summary>
    public async Task StopConsumingAsync()
    {
        await listener.StopConsumingAsync().ConfigureAwait(false);
        await eventListener.StopConsumingAsync().ConfigureAwait(false);
        // A drained service has no stream left to cancel.
        await cancelListener.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>Stop consuming and close the service's channels.</summary>
    public async ValueTask DisposeAsync()
    {
        await StopConsumingAsync().ConfigureAwait(false);
        await CloseQuietlyAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private async Task CloseQuietlyAsync()
    {
        foreach (BaseListener l in new BaseListener[] { listener, eventListener })
        {
            try
            {
                if (l.IsInitialized) await l.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Debug($"closing {l.GetType().Name}: {e.Message}");
            }
        }
        await cancelListener.CloseAsync().ConfigureAwait(false);
    }

    // ---- events ------------------------------------------------------------------------

    /// <summary>Publish an event of the message's own type, on <c>EVENT.&lt;type&gt;</c> unless a topic is given.</summary>
    public Task PublishEventAsync(IMessage content, string? topic = null, CancellationToken cancellationToken = default) =>
        Context.PublishEventAsync(content, topic, cancellationToken);

    /// <summary>
    /// Subscribe this service to events of <typeparamref name="T"/>, on <c>EVENT.&lt;type&gt;</c>
    /// or a topic or pattern of your choosing. Call after <see cref="InitAsync"/>.
    /// </summary>
    public Task SubscribeEventAsync<T>(EventCallback<T> handler, string? topic = null) where T : IMessage<T>, new() =>
        eventListener.SubscribeAsync(handler, topic);

    // ---- dispatch ----------------------------------------------------------------------

    private static string LastSegment(string value)
    {
        var i = value.LastIndexOf('.');
        return i < 0 ? value : value.Substring(i + 1);
    }

    /// <summary>The core handler for requests made to <c>REQUEST.&lt;ServiceName&gt;.*</c>.</summary>
    private async Task<HandlerResult> OnMessage(byte[] data, string id, MessageHandlerContext delivery)
    {
        ResolveContract();
        var factory = Context.Factory;
        var routingKey = delivery.RoutingKey;
        var contract = contractServiceName!;

        // Envelope first, payload later: the envelope names the method, and that name selects the
        // schema the payload is read with, so it is checked against this service's contract
        // before the bytes are interpreted.
        Envelopes.Request envelope;
        try
        {
            envelope = Envelopes.DecodeRequest(data);
        }
        catch (Envelopes.MalformedException)
        {
            Logger.Error($"unparseable request envelope on {ServiceName} ({data.Length} bytes, {id})");
            return ProtocolErrorReply(routingKey, "request envelope did not decode");
        }
        Logger.Debug($"received request {envelope.Method} ({id})");

        // A rejection is reported against the method the ROUTING KEY names: the body's name is
        // exactly what is in dispute.
        var contractMethod = contract + "."
            + (!string.IsNullOrEmpty(routingKey) ? LastSegment(routingKey) : LastSegment(envelope.Method));

        // 1. The delivery belongs to THIS service, by the key the broker used.
        // 2. The body asks for the method the routing key names, so a client that can publish
        //    cannot route to one method and have another executed.
        if (!string.IsNullOrEmpty(routingKey))
        {
            if (!routingKey.StartsWith("REQUEST." + ServiceName + ".", StringComparison.Ordinal))
                return RejectDispatch(contractMethod, $"routing key {routingKey} does not belong to service {ServiceName}");
            if (LastSegment(routingKey) != LastSegment(envelope.Method))
                return RejectDispatch(contractMethod, $"request method {envelope.Method} contradicts routing key {routingKey}");
        }
        // 3. The body names a method of THIS contract, spelled in full.
        string method;
        try
        {
            var parsed = MessageFactory.SplitMethodName(envelope.Method);
            if (parsed.ServiceName != contract)
                return RejectDispatch(contractMethod, $"request method {envelope.Method} is not a method of {contract}");
            method = parsed.MethodName;
        }
        catch (InvalidMethodNameError)
        {
            return RejectDispatch(contractMethod, $"request method {envelope.Method} is not a qualified method name");
        }
        if (!declaredMethods.Contains(method))
            return RejectDispatch(contractMethod, $"{contract} declares no method {method}");

        MethodEntry? entry;
        lock (sync) methods.TryGetValue(method, out entry);
        var fullMethod = envelope.Method;
        if (entry == null)
        {
            var error = new InvalidMethodError("invalid service method " + method);
            Logger.Error(error.Message);
            return HandlerResult.FromReply(MessageFactory.BuildErrorResponse(fullMethod, error));
        }
        var descriptor = factory.Method(fullMethod);
        if (entry.Streaming != descriptor.IsServerStreaming)
        {
            var error = new InvalidMethodError($"method {method} is registered as {(entry.Streaming ? "streaming" : "unary")} "
                + "but declared otherwise");
            Logger.Error(error.Message);
            return HandlerResult.FromReply(MessageFactory.BuildErrorResponse(fullMethod, error));
        }

        // Validated: the payload can now be read against the contract's schema.
        object request;
        try
        {
            request = entry.Decode(envelope.Data);
        }
        catch (Exception e) when (e is InvalidProtocolBufferException or SchemaError or CustomTypeRangeError)
        {
            // Type name and size only: a payload that failed to decode is still a payload.
            Logger.Error($"unparseable request payload for {fullMethod} ({envelope.Data.Length} bytes, {id})");
            return ProtocolErrorReply(fullMethod, $"payload did not decode as the request type of {fullMethod}"
                + (e is CustomTypeRangeError ? ": " + e.Message : ""));
        }
        var call = new CallContext(envelope.Actor ?? "", id, fullMethod, delivery);

        if (entry.Streaming)
            return HandlerResult.FromStream(RunStream(entry, fullMethod, descriptor, request, call));
        object result;
        try
        {
            result = await entry.Unary!(request, call).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            return HandleUnaryError(fullMethod, error, id);
        }
        try
        {
            var payload = EncodeResult(descriptor, result);
            Logger.Debug($"sending result {fullMethod} ({id})");
            return HandlerResult.FromReply(MessageFactory.BuildEncodedResponse(fullMethod, payload));
        }
        catch (Exception error)
        {
            return HandleUnaryError(fullMethod, error, id);
        }
    }

    private static byte[] EncodeResult(MethodDescriptor descriptor, object? result)
    {
        switch (result)
        {
            case null:
                throw new InvalidResultError(descriptor.FullName + " returned null");
            case byte[] raw:
                return raw;
            case IMessage message:
                var expected = descriptor.OutputType.FullName;
                var actual = message.Descriptor.FullName;
                if (expected != actual) throw new InvalidResultError($"{descriptor.FullName} must return {expected}, not {actual}");
                return MessageFactory.EncodeMessage(message);
            default:
                throw new InvalidResultError($"{descriptor.FullName} returned a {result.GetType().Name}");
        }
    }

    /// <summary>
    /// Produce a streaming reply. A chunk is a ResponseContainer; a failure, at any point, ends the
    /// stream with a terminal error chunk, sanitised like a unary error, which the caller's
    /// iteration raises. Streams are not retried.
    /// </summary>
    private static async IAsyncEnumerable<byte[]> RunStream(MethodEntry entry, string fullMethod,
        MethodDescriptor descriptor, object request, CallContext call,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        byte[]? terminal = null;
        IAsyncEnumerator<object>? chunks = null;
        try
        {
            chunks = entry.Stream!(request, call).GetAsyncEnumerator(cancellationToken);
        }
        catch (Exception error)
        {
            terminal = StreamFailure(fullMethod, error, call);
        }
        if (chunks != null)
        {
            await using (chunks.ConfigureAwait(false))
            {
                while (true)
                {
                    byte[] chunk;
                    try
                    {
                        if (!await chunks.MoveNextAsync().ConfigureAwait(false)) break;
                        chunk = MessageFactory.BuildEncodedResponse(fullMethod, EncodeResult(descriptor, chunks.Current));
                    }
                    catch (Exception error)
                    {
                        terminal = StreamFailure(fullMethod, error, call);
                        break;
                    }
                    yield return chunk;
                }
            }
        }
        if (terminal != null) yield return terminal;
    }

    /// <summary>The terminal error chunk for a failed stream; a cancelled one ends without one.</summary>
    private static byte[] StreamFailure(string fullMethod, Exception error, CallContext call)
    {
        if (error is Connection.StreamCancelledException || call.CancellationToken.IsCancellationRequested)
            throw new Connection.StreamCancelledException();
        if (Errors.IsHandledError(error)) Logger.Warn($"handled error in stream {fullMethod}: {Errors.MessageOf(error)}");
        else Logger.Error(error.ToString());
        return MessageFactory.BuildErrorResponse(fullMethod, Errors.SanitizeErrorForClient(error, call.CorrelationId));
    }

    /// <summary>
    /// Answer a message this service could not understand. A ProtocolError, so it is replied to
    /// and rejected instead of retried: the same bytes fail the same way on every redelivery.
    /// </summary>
    private static HandlerResult ProtocolErrorReply(string? label, string reason) =>
        HandlerResult.FromReply(MessageFactory.BuildErrorResponse(string.IsNullOrEmpty(label) ? "unknown" : label,
            new ProtocolError(reason)));

    private static HandlerResult RejectDispatch(string contractMethod, string reason)
    {
        Logger.Error(reason);
        return HandlerResult.FromReply(MessageFactory.BuildErrorResponse(contractMethod, new InvalidMethodError(reason)));
    }

    /// <summary>
    /// A <see cref="HandledError"/> is expected: it is answered at once, never retried. Anything
    /// else is an infrastructure failure, rethrown for the retry ladder with the caller's
    /// (sanitised) reply pre-encoded, for the connection to send once the retries are spent.
    /// </summary>
    private static HandlerResult HandleUnaryError(string method, Exception error, string correlationId)
    {
        if (Errors.IsHandledError(error))
        {
            Logger.Warn($"handled error in {method}: {Errors.MessageOf(error)}");
            return HandlerResult.FromReply(MessageFactory.BuildErrorResponse(method, error));
        }
        Logger.Error(error.ToString());
        var reply = MessageFactory.BuildErrorResponse(method, Errors.SanitizeErrorForClient(error, correlationId));
        throw new ErrorWithReply(error, reply);
    }

    /// <summary>The caller's answer to a processing timeout: PROCESSING_TIMEOUT, once the retries are spent.</summary>
    private static byte[]? BuildTimeoutReply(byte[] content, Exception error)
    {
        if (error is not TimeoutError) return null;
        string method;
        try
        {
            method = Envelopes.DecodeRequest(content).Method;
        }
        catch (Exception)
        {
            method = "unknown";
        }
        return MessageFactory.BuildErrorResponse(method, error);
    }

    /// <summary>For <see cref="RunnableService"/>.</summary>
    internal MessageListener RequestListener => listener;
}
