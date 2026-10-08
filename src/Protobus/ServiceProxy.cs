using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Protobus.Internal;

namespace Protobus;

/// <summary>
/// Calls a service on the bus by name.
///
/// Generated <c>&lt;Service&gt;Protobus.Proxy</c> classes wrap one with a typed method per rpc.
/// Used directly, it calls methods by name, typed with a <see cref="MessageParser{T}"/> or over
/// raw payload bytes:
/// <code>
/// var calc = new ServiceProxy(ctx, "Calculator.Service");
/// calc.Init();
/// var reply = await calc.CallAsync("add", new AddRequest { A = 20, B = 22 }, AddResponse.Parser);
/// </code>
/// The proxy may be constructed with an instance name (<c>Combat.Player.player6</c>) for a
/// service registered under one: requests route to <c>REQUEST.&lt;instance name&gt;.&lt;method&gt;</c>,
/// and the envelope names the contract method, which is what the service validates against.
/// </summary>
public class ServiceProxy
{
    private readonly Context context;
    private volatile string? contract;
    private volatile HashSet<string> methods = new();
    private volatile HashSet<string> streaming = new();
    private volatile bool initialized;
    private readonly object sync = new();

    public ServiceProxy(Context context, string serviceName)
    {
        this.context = context;
        ServiceName = serviceName;
    }

    public string ServiceName { get; }

    public string? ContractServiceName => contract;

    public bool IsInitialized => initialized;

    public IReadOnlyCollection<string> Methods => methods;

    public bool IsStreaming(string method) => streaming.Contains(method);

    private string ResolveContract()
    {
        var factory = context.Factory;
        var candidate = ServiceName;
        while (true)
        {
            if (factory.HasService(candidate))
            {
                // Said out loud, because trimming is a guess.
                if (candidate != ServiceName)
                    Logger.Info($"service proxy '{ServiceName}' resolved to contract '{candidate}'; requests will route to "
                        + $"REQUEST.{ServiceName}.*");
                return candidate;
            }
            var cut = candidate.LastIndexOf('.');
            if (cut <= 0)
                throw new InvalidServiceNameError($"no service in the schema matches '{ServiceName}' or any prefix of it; "
                    + "the schema must declare the service this proxy addresses");
            candidate = candidate.Substring(0, cut);
        }
    }

    /// <summary>Resolve the contract and the methods it declares.</summary>
    /// <exception cref="InvalidServiceNameError">no known service matches the name or a prefix of it</exception>
    /// <exception cref="AlreadyInitializedError">on a second call</exception>
    public void Init()
    {
        lock (sync)
        {
            if (initialized)
            {
                Logger.Error("already initialized service proxy " + ServiceName);
                throw new AlreadyInitializedError();
            }
            var resolved = ResolveContract();
            var service = context.Factory.Service(resolved);
            var all = new HashSet<string>();
            var streams = new HashSet<string>();
            foreach (var m in service.Methods)
            {
                all.Add(m.Name);
                if (m.IsServerStreaming) streams.Add(m.Name);
            }
            contract = resolved;
            methods = all;
            streaming = streams;
            initialized = true;
        }
    }

    private void RequireMethod(string method, bool stream)
    {
        if (!initialized) throw new NotInitializedError($"service proxy {ServiceName} is not initialized");
        if (!methods.Contains(method)) throw new UnknownMethodError($"service '{contract}' declares no method '{method}'");
        if (stream != streaming.Contains(method))
            throw new InvalidRequestError($"{contract}.{method} is "
                + (stream ? "unary; use CallAsync()" : "server-streaming; use CallStream()"));
    }

    private byte[] BuildRequest(string fullMethod, IMessage request, string? actor)
    {
        try
        {
            return context.Factory.BuildRequest(fullMethod, request, actor);
        }
        catch (Exception e) when (e is not InvalidRequestError)
        {
            // Type and error only: the request is application data.
            Logger.Error($"failed building request for {fullMethod}: {e.Message}");
            throw new InvalidRequestError("failed parsing message: " + e.Message);
        }
    }

    // ---- unary -------------------------------------------------------------------------

    /// <summary>
    /// Call a unary method. A service error fails with <see cref="RemoteError"/>; a delivery
    /// failure with its <see cref="PublishError"/> unchanged. With <see cref="CallOptions.Rpc"/>
    /// false the call completes with an empty message once the request is confirmed.
    /// </summary>
    public async Task<TResponse> CallAsync<TResponse>(string method, IMessage request, MessageParser<TResponse> parser,
        CallOptions? options = null, CancellationToken cancellationToken = default) where TResponse : IMessage<TResponse>
    {
        var o = options ?? CallOptions.Default;
        RequireMethod(method, false);
        var full = contract + "." + method;
        var content = BuildRequest(full, request, o.Actor);
        var reply = await context.PublishMessageAsync(content, RoutingKey(method), o, cancellationToken).ConfigureAwait(false);
        if (!o.Rpc)
        {
            Logger.Debug($"non-rpc call to {full} confirmed");
            return parser.ParseFrom(Array.Empty<byte>());
        }
        return Decode(full, ResultPayload(full, reply!), parser);
    }

    /// <summary>
    /// Call a unary method with a serialized request; returns the serialized response, or an empty
    /// array when <see cref="CallOptions.Rpc"/> is false. Nothing checks either payload's type.
    /// </summary>
    public async Task<byte[]> CallRawAsync(string method, byte[] payload, CallOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var o = options ?? CallOptions.Default;
        RequireMethod(method, false);
        var full = contract + "." + method;
        var content = Envelopes.EncodeRequest(new Envelopes.Request(full, o.Actor, payload));
        var reply = await context.PublishMessageAsync(content, RoutingKey(method), o, cancellationToken).ConfigureAwait(false);
        return o.Rpc ? ResultPayload(full, reply!) : Array.Empty<byte>();
    }

    private string RoutingKey(string method) => "REQUEST." + ServiceName + "." + method;

    /// <summary>Typed decode with a generated message's parser, checking its custom-type values.</summary>
    private static T Decode<T>(string fullMethod, byte[] data, MessageParser<T> parser) where T : IMessage<T>
    {
        try
        {
            var value = parser.ParseFrom(data);
            CustomTypes.Validate(value);
            return value;
        }
        catch (Exception e) when (e is InvalidProtocolBufferException or CustomTypeRangeError)
        {
            throw new InvalidResponseError($"failed parsing result for {fullMethod}: {e.Message}");
        }
    }

    /// <summary>The result payload of a reply, or the service's error as a <see cref="RemoteError"/>.</summary>
    internal static byte[] ResultPayload(string fullMethod, byte[] reply)
    {
        Envelopes.Response response;
        try
        {
            response = Envelopes.DecodeResponse(reply);
        }
        catch (Envelopes.MalformedException)
        {
            throw new InvalidResponseError("failed parsing result for " + fullMethod);
        }
        if (response.Error is { } e) throw new RemoteError(e.Message, e.Code, e.Method);
        return response.Result?.Data ?? Array.Empty<byte>();
    }

    // ---- streaming ---------------------------------------------------------------------

    /// <summary>
    /// Call a server-streaming method. The request is published when enumeration starts; leaving
    /// the loop early, or cancelling, tells the producer to stop. A terminal error chunk is raised
    /// as <see cref="RemoteError"/>.
    /// </summary>
    public async IAsyncEnumerable<TResponse> CallStream<TResponse>(string method, IMessage request,
        MessageParser<TResponse> parser, StreamOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where TResponse : IMessage<TResponse>
    {
        var full = contract + "." + method;
        await foreach (var payload in CallStreamRaw(method, null, request, options, cancellationToken).ConfigureAwait(false))
            yield return Decode(full, payload, parser);
    }

    /// <summary>A server-streaming call with a serialized request, yielding serialized chunks.</summary>
    public IAsyncEnumerable<byte[]> CallStreamRaw(string method, byte[] payload, StreamOptions? options = null,
        CancellationToken cancellationToken = default) =>
        CallStreamRaw(method, payload, null, options, cancellationToken);

    private async IAsyncEnumerable<byte[]> CallStreamRaw(string method, byte[]? payload, IMessage? request,
        StreamOptions? options, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var o = options ?? StreamOptions.Default;
        RequireMethod(method, true);
        var full = contract + "." + method;
        var content = request != null
            ? BuildRequest(full, request, o.Actor)
            : Envelopes.EncodeRequest(new Envelopes.Request(full, o.Actor, payload!));
        await using var chunks = context.PublishStreamingMessage(content, RoutingKey(method), o, cancellationToken);
        await foreach (var chunk in ((IAsyncEnumerable<byte[]>)chunks).ConfigureAwait(false))
        {
            Envelopes.Response response;
            try
            {
                response = Envelopes.DecodeResponse(chunk);
            }
            catch (Envelopes.MalformedException)
            {
                throw new InvalidResponseError("failed parsing streaming chunk for " + full);
            }
            // A terminal chunk may carry an error instead of a result.
            if (response.Error is { } e) throw new RemoteError(e.Message, e.Code, e.Method);
            yield return response.Result?.Data ?? Array.Empty<byte>();
        }
    }
}
