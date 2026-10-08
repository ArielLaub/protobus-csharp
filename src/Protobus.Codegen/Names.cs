using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Protobuf.Reflection;

namespace Protobus.Codegen;

/// <summary>C# names for protobuf declarations, by protoc's own rules for --csharp_out.</summary>
internal static class Names
{
    /// <summary>
    /// Members of the generated classes and their supertypes (MessageService, RunnableService,
    /// ServiceProxy, object) that an rpc's method must not shadow.
    /// </summary>
    internal static readonly HashSet<string> ReservedMembers = new()
    {
        "Base", "Proxy", "Descriptor", "ServiceFullName",
        "InitAsync", "DisposeAsync", "ServiceName", "ContractServiceName", "Options", "Context", "Schema",
        "PublishEventAsync", "SubscribeEventAsync", "StopConsumingAsync", "CleanupAsync", "RegisterUnary",
        "RegisterStream", "RegisterMethod", "RegisterStreamingMethod", "StartAsync", "RequestShutdown",
        "WaitForShutdownAsync", "NewProxy", "InitProxy", "RequestListener",
        "Init", "CallAsync", "CallRawAsync", "CallStream", "CallStreamRaw", "IsInitialized", "Methods", "IsStreaming",
        "Equals", "GetHashCode", "ToString", "GetType", "MemberwiseClone", "Finalize", "ReferenceEquals",
    };

    /// <summary>protoc's UnderscoresToPascalCase: a capital after every non-letter, digits kept.</summary>
    internal static string Pascal(string name)
    {
        var o = new StringBuilder();
        var capNext = true;
        foreach (var c in name)
        {
            if (c is >= 'a' and <= 'z')
            {
                o.Append(capNext ? char.ToUpperInvariant(c) : c);
                capNext = false;
            }
            else if (c is >= 'A' and <= 'Z')
            {
                o.Append(c);
                capNext = false;
            }
            else if (c is >= '0' and <= '9')
            {
                o.Append(c);
                capNext = true;
            }
            else
            {
                capNext = true;
            }
        }
        return o.ToString();
    }

    /// <summary>The file's C# namespace: csharp_namespace, or the package with each segment PascalCased.</summary>
    internal static string Namespace(FileDescriptor file)
    {
        var options = file.GetOptions();
        if (options != null && options.HasCsharpNamespace) return options.CsharpNamespace;
        return string.Join(".", file.Package.Split('.').Where(s => s.Length > 0).Select(Pascal));
    }

    private static string Qualify(string ns, string name) => "global::" + (ns.Length == 0 ? "" : ns + ".") + name;

    /// <summary>The class holding a file's descriptor.</summary>
    internal static string ReflectionClass(FileDescriptor file)
    {
        var name = file.Name;
        var slash = name.LastIndexOf('/');
        if (slash >= 0) name = name.Substring(slash + 1);
        if (name.EndsWith(".protodevel")) name = name.Substring(0, name.Length - ".protodevel".Length);
        else if (name.EndsWith(".proto")) name = name.Substring(0, name.Length - ".proto".Length);
        return Qualify(Namespace(file), Pascal(name) + "Reflection");
    }

    /// <summary>The fully-qualified C# class of a message; nested types live in their parent's <c>Types</c> class.</summary>
    internal static string MessageClass(MessageDescriptor d)
    {
        var nested = d.Name;
        for (var p = d.ContainingType; p != null; p = p.ContainingType) nested = p.Name + ".Types." + nested;
        return Qualify(Namespace(d.File), nested);
    }
}
