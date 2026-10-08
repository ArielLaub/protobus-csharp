using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Protobus.Internal;

namespace Protobus;

/// <summary>
/// The schemas a context knows, and the encoding of requests, replies and events.
/// </summary>
/// <remarks>
/// Generated code registers its own schema (<see cref="Register"/>), so a program built with the
/// protobus code generator needs nothing loaded at runtime. Google.Protobuf for C# has no dynamic
/// messages, so the dynamic API works on serialized payloads: see <see cref="ServiceProxy"/>.
/// Thread-safe.
/// </remarks>
public sealed class MessageFactory
{
    private readonly object sync = new();
    private readonly Dictionary<string, FileDescriptor> files = new();
    private readonly Dictionary<string, MessageDescriptor> messages = new();
    private readonly Dictionary<string, ServiceDescriptor> services = new();

    public MessageFactory() => Register(Types.TypesReflection.Descriptor);

    /// <summary>
    /// Register a compiled schema and, first, everything it imports. Idempotent per file name:
    /// generated code registers its schema each time a service or proxy is built.
    /// </summary>
    /// <exception cref="SchemaError">when the file declares a name another registered file declares</exception>
    public void Register(FileDescriptor file)
    {
        lock (sync)
        {
            if (files.ContainsKey(file.Name)) return;
            foreach (var dep in file.Dependencies) Register(dep);
            var newMessages = new List<MessageDescriptor>();
            foreach (var d in file.MessageTypes) Collect(d, newMessages);
            foreach (var d in newMessages)
                if (messages.TryGetValue(d.FullName, out var existing))
                    throw new SchemaError($"type {d.FullName} in {file.Name} is already declared by {existing.File.Name}");
            foreach (var s in file.Services)
                if (services.TryGetValue(s.FullName, out var existing))
                    throw new SchemaError($"service {s.FullName} in {file.Name} is already declared by {existing.File.Name}");
            files[file.Name] = file;
            foreach (var d in newMessages) messages[d.FullName] = d;
            foreach (var s in file.Services) services[s.FullName] = s;
        }
        Logger.Debug("registered schema " + file.Name);
    }

    private static void Collect(MessageDescriptor d, List<MessageDescriptor> into)
    {
        into.Add(d);
        foreach (var n in d.NestedTypes) Collect(n, into);
    }

    public bool HasService(string fullName)
    {
        lock (sync) return services.ContainsKey(fullName);
    }

    public bool HasType(string fullName)
    {
        lock (sync) return messages.ContainsKey(fullName);
    }

    public ServiceDescriptor Service(string fullName)
    {
        lock (sync)
            return services.TryGetValue(fullName, out var s) ? s : throw new InvalidServiceNameError("no such service " + fullName);
    }

    public MessageDescriptor Type(string fullName)
    {
        lock (sync)
            return messages.TryGetValue(fullName, out var m) ? m : throw new SchemaError("no such message type " + fullName);
    }

    /// <summary>Method names a service declares, in declaration order.</summary>
    public IReadOnlyList<string> ServiceMethodNames(string serviceFullName) =>
        Service(serviceFullName).Methods.Select(m => m.Name).ToList();

    /// <summary>
    /// Split <c>&lt;package&gt;.&lt;Service&gt;.&lt;method&gt;</c> from the right, so a dotted
    /// package splits into service <c>com.example.Calc</c> and method <c>add</c>.
    /// </summary>
    public static (string ServiceName, string MethodName) SplitMethodName(string? fullName)
    {
        var i = fullName?.LastIndexOf('.') ?? -1;
        if (i <= 0 || i == fullName!.Length - 1)
            throw new InvalidMethodNameError($"'{fullName}' is not a fully-qualified method name (<package>.<Service>.<method>)");
        return (fullName.Substring(0, i), fullName.Substring(i + 1));
    }

    public MethodDescriptor Method(string fullName)
    {
        var (serviceName, methodName) = SplitMethodName(fullName);
        lock (sync)
        {
            if (!services.TryGetValue(serviceName, out var s)) throw new UnknownMethodError($"no such service '{serviceName}'");
            return s.FindMethodByName(methodName)
                ?? throw new UnknownMethodError($"service '{serviceName}' declares no method '{methodName}'");
        }
    }

    /// <summary>Whether the method is declared server-streaming. Unknown methods are treated as unary.</summary>
    public bool IsStreamingMethod(string fullName)
    {
        try
        {
            return Method(fullName).IsServerStreaming;
        }
        catch (ProtobusException e)
        {
            Logger.Debug($"IsStreamingMethod({fullName}): treating as unary ({e.Message})");
            return false;
        }
    }

    // ---- messages ----------------------------------------------------------------------

    /// <summary>Serialize a message after checking its custom-type values.</summary>
    public static byte[] EncodeMessage(IMessage message)
    {
        CustomTypes.Validate(message);
        return message.ToByteArray();
    }

    /// <summary>Encode a request: its payload wrapped in a RequestContainer.</summary>
    public byte[] BuildRequest(string methodFullName, IMessage request, string? actor)
    {
        var m = Method(methodFullName);
        if (request.Descriptor.FullName != m.InputType.FullName)
            throw new InvalidRequestError($"the request of {methodFullName} must be {m.InputType.FullName}, got {request.Descriptor.FullName}");
        return Envelopes.EncodeRequest(new Envelopes.Request(methodFullName, actor, EncodeMessage(request)));
    }

    /// <summary>Encode a successful reply whose payload is already serialized.</summary>
    public static byte[] BuildEncodedResponse(string methodFullName, byte[] payload) =>
        Envelopes.EncodeResponse(new Envelopes.Response(new Envelopes.Result(methodFullName, payload), null));

    /// <summary>
    /// Encode an error reply. No method lookup: an error carries the method only as a label, so a
    /// failure that is about an unknown method can still be reported.
    /// </summary>
    public static byte[] BuildErrorResponse(string? methodFullName, Exception error) =>
        BuildErrorResponse(methodFullName, Errors.MessageOf(error), Errors.CodeOf(error));

    public static byte[] BuildErrorResponse(string? methodFullName, string? message, string? code) =>
        Envelopes.EncodeResponse(new Envelopes.Response(null,
            new Envelopes.ErrorReply(methodFullName ?? "", message ?? "", code ?? "")));

    /// <summary>Encode an event; <paramref name="type"/> is the payload's full message name.</summary>
    public static byte[] BuildEvent(string type, IMessage content, string topic) =>
        Envelopes.EncodeEvent(new Envelopes.Event(type, topic, EncodeMessage(content)));
}
