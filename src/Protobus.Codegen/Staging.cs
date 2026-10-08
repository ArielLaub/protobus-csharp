using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Protobus.Codegen;

/// <summary>
/// Copies shared schemas into a staging directory and adds what protoc needs.
///
/// Shared protobus schemas use the custom types without importing them. Each staged copy gains,
/// at its end (so line numbers in protoc's errors still match the original),
/// <c>import "protobus/types.proto";</c> when it uses <c>bigint</c> or <c>timestamp</c> without
/// declaring them, and the import of each declared custom type it uses. The original files are
/// never touched.
/// </summary>
internal static class Staging
{
    internal const string Types = "protobus/types.proto";
    internal static readonly string[] Builtin = { "bigint", "timestamp" };

    /// <summary>A custom type of the user's: <c>message NAME { optional WIRE value = 1; }</c>.</summary>
    internal sealed record CustomType(string Name, string Wire)
    {
        internal static readonly string[] Wires = { "bytes", "int64", "uint64", "string", "int32", "uint32", "double" };

        internal static CustomType Parse(string spec)
        {
            var colon = spec.IndexOf(':');
            if (colon <= 0) throw new ArgumentException("--custom-type wants NAME:WIRE, got " + spec);
            var name = spec.Substring(0, colon);
            var wire = spec.Substring(colon + 1);
            if (!Regex.IsMatch(name, "^[A-Za-z_][A-Za-z0-9_]*$")) throw new ArgumentException("bad custom type name " + name);
            if (!Wires.Contains(wire))
                throw new ArgumentException($"custom type {name}: wire type must be one of {string.Join(", ", Wires)}");
            if (Builtin.Contains(name)) throw new ArgumentException(name + " is a built-in custom type");
            return new CustomType(name, wire);
        }

        internal string File => "protobus/custom/" + Name + ".proto";
    }

    internal sealed record Staged(string Root, List<string> Schemas, List<string> GeneratedTypes);

    /// <summary>Text with comments and string literals blanked, so names inside them do not count.</summary>
    internal static string Code(string proto) => Strip(proto, true);

    /// <summary>Text with comments blanked, string literals kept: for reading import paths.</summary>
    internal static string WithoutComments(string proto) => Strip(proto, false);

    private static string Strip(string proto, bool blankStrings)
    {
        var o = new StringBuilder(proto.Length);
        var i = 0;
        var n = proto.Length;
        while (i < n)
        {
            var c = proto[i];
            if (c == '/' && i + 1 < n && proto[i + 1] == '/')
            {
                while (i < n && proto[i] != '\n')
                {
                    o.Append(' ');
                    i++;
                }
            }
            else if (c == '/' && i + 1 < n && proto[i + 1] == '*')
            {
                var end = proto.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var stop = end < 0 ? n : end + 2;
                while (i < stop)
                {
                    o.Append(proto[i] == '\n' ? '\n' : ' ');
                    i++;
                }
            }
            else if (c == '"' || c == '\'')
            {
                // A literal is copied (or blanked) whole, so a "//" inside one is never read as a
                // comment.
                o.Append(c);
                i++;
                while (i < n && proto[i] != c)
                {
                    if (proto[i] == '\\' && i + 1 < n)
                    {
                        o.Append(blankStrings ? "  " : proto.Substring(i, 2));
                        i += 2;
                        continue;
                    }
                    o.Append(blankStrings ? ' ' : proto[i]);
                    i++;
                }
                if (i < n)
                {
                    o.Append(c);
                    i++;
                }
            }
            else
            {
                o.Append(c);
                i++;
            }
        }
        return o.ToString();
    }

    /// <summary>Whether the schema uses <paramref name="type"/> as a field, map value or rpc type.</summary>
    internal static bool Uses(string code, string type) =>
        Regex.IsMatch(code, @"(?<![\w.])\.?" + Regex.Escape(type) + @"(?=\s+[A-Za-z_]\w*\s*=|\s*>|\s*\))");

    internal static bool Declares(string code, string type) => Regex.IsMatch(code, @"\bmessage\s+" + Regex.Escape(type) + @"\b");

    internal static bool Imports(string code, string file) =>
        Regex.Matches(code, "\\bimport\\s+(?:public\\s+|weak\\s+)?\"([^\"]*)\"").Any(m => m.Groups[1].Value == file);

    /// <summary>The staged text of one schema.</summary>
    internal static string Stage(string proto, IReadOnlyList<CustomType> custom)
    {
        var code = Code(proto);
        var importable = WithoutComments(proto);
        var extra = new StringBuilder();
        var builtin = Builtin.Any(t => Uses(code, t) && !Declares(code, t));
        if (builtin && !Imports(importable, Types)) extra.Append("import \"").Append(Types).Append("\";\n");
        foreach (var t in custom)
            if (Uses(code, t.Name) && !Declares(code, t.Name) && !Imports(importable, t.File))
                extra.Append("import \"").Append(t.File).Append("\";\n");
        if (extra.Length == 0) return proto;
        var sep = proto.EndsWith("\n", StringComparison.Ordinal) ? "" : "\n";
        return proto + sep + "// Added by protobus-codegen to its staged copy.\n" + extra;
    }

    internal static string CustomTypeSchema(CustomType t, string csharpNamespace) =>
        "// A protobus custom type, declared at the root like bigint and timestamp.\n"
        + "syntax = \"proto3\";\n\n"
        + "option csharp_namespace = \"" + csharpNamespace + "\";\n\n"
        + "message " + t.Name + " {\n  optional " + t.Wire + " value = 1;\n}\n";

    /// <summary>The generator's copy of protobus/types.proto.</summary>
    internal static string TypesSchema()
    {
        using var s = typeof(Staging).Assembly.GetManifestResourceStream(Types)
            ?? throw new IOException("the generator's copy of " + Types + " is missing");
        using var r = new StreamReader(s, Encoding.UTF8);
        return r.ReadToEnd();
    }

    /// <summary>
    /// Stage every <c>.proto</c> under <paramref name="dirs"/> into <paramref name="root"/>, keyed
    /// by its path relative to its directory, plus protobus/types.proto and the custom types'
    /// schemas.
    /// </summary>
    internal static Staged Stage(IReadOnlyList<string> dirs, string root, IReadOnlyList<CustomType> custom,
        string customNamespace)
    {
        var found = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var dir in dirs)
        {
            if (!Directory.Exists(dir)) throw new IOException($"proto directory {dir} does not exist");
            foreach (var p in Directory.EnumerateFiles(dir, "*.proto", SearchOption.AllDirectories).OrderBy(x => x, StringComparer.Ordinal))
            {
                var rel = Path.GetRelativePath(dir, p).Replace('\\', '/');
                if (rel == Types || rel.StartsWith("protobus/custom/", StringComparison.Ordinal)) continue;
                if (found.TryGetValue(rel, out var previous))
                    throw new IOException($"{rel} is in more than one proto directory: {previous} and {p}");
                found[rel] = p;
            }
        }
        var schemas = new List<string>();
        foreach (var (rel, source) in found)
        {
            var target = Path.Combine(root, rel);
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            File.WriteAllText(target, Stage(File.ReadAllText(source, Encoding.UTF8), custom), new UTF8Encoding(false));
            schemas.Add(rel);
        }
        var types = Path.Combine(root, Types);
        Directory.CreateDirectory(Path.GetDirectoryName(types)!);
        File.WriteAllText(types, TypesSchema(), new UTF8Encoding(false));
        var generatedTypes = new List<string>();
        foreach (var t in custom)
        {
            var p = Path.Combine(root, t.File);
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);
            File.WriteAllText(p, CustomTypeSchema(t, customNamespace), new UTF8Encoding(false));
            generatedTypes.Add(t.File);
        }
        return new Staged(root, schemas, generatedTypes);
    }
}
