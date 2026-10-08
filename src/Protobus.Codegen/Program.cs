using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.Compiler;
using Google.Protobuf.Reflection;

namespace Protobus.Codegen;

/// <summary>
/// The protobus code generator.
/// <code>
/// protobus-csharp generate --proto-dir DIR... --out DIR [--descriptor-out FILE]
///                          [--custom-type NAME:WIRE]... [--custom-type-namespace NS] [--protoc PATH]
/// </code>
/// <c>generate</c> stages the schemas (see <see cref="Staging"/>), runs protoc with
/// <c>--csharp_out</c>, and writes a <c>&lt;Service&gt;Protobus</c> class per service. protoc is
/// <c>--protoc</c>, then <c>$PROTOC</c>, then the one on the PATH.
///
/// Run with no arguments, it is a protoc plugin (<c>protoc-gen-protobus-csharp</c>): it reads a
/// CodeGeneratorRequest on stdin and writes the protobus classes for the files protoc asks for.
/// Schemas compiled that way import the custom types themselves:
/// <c>import "protobus/types.proto";</c>, which ships in the Protobus package.
/// </summary>
public static class Program
{
    internal const string Usage =
        "usage: protobus-csharp generate --proto-dir DIR... --out DIR [--descriptor-out FILE]\n"
        + "                                [--custom-type NAME:WIRE]... [--custom-type-namespace NS] [--protoc PATH]\n"
        + "       protobus-csharp            (no arguments: run as a protoc plugin)";

    public static int Main(string[] args)
    {
        if (args.Length == 0) return Plugin();
        try
        {
            return Run(args, Console.Out, Console.Error);
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine("protobus-csharp: " + e.Message);
            Console.Error.WriteLine(Usage);
            return 2;
        }
        catch (IOException e)
        {
            Console.Error.WriteLine("protobus-csharp: " + e.Message);
            return 1;
        }
    }

    internal static int Run(string[] args, TextWriter stdout, TextWriter stderr)
    {
        if (args[0] != "generate") throw new ArgumentException("unknown command " + args[0]);
        var dirs = new List<string>();
        var custom = new List<Staging.CustomType>();
        string? outDir = null;
        string? descriptorOut = null;
        string? protoc = null;
        var customNamespace = "Protobus.Custom";
        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];
            if (i + 1 >= args.Length) throw new ArgumentException(a + " needs a value");
            var v = args[++i];
            switch (a)
            {
                case "--proto-dir": dirs.Add(v); break;
                case "--out": outDir = v; break;
                case "--descriptor-out": descriptorOut = v; break;
                case "--custom-type": custom.Add(Staging.CustomType.Parse(v)); break;
                case "--custom-type-namespace": customNamespace = v; break;
                case "--protoc": protoc = v; break;
                default: throw new ArgumentException("unknown option " + a);
            }
        }
        if (dirs.Count == 0) throw new ArgumentException("at least one --proto-dir is required");
        if (outDir == null) throw new ArgumentException("--out is required");
        if (string.IsNullOrEmpty(protoc)) protoc = Environment.GetEnvironmentVariable("PROTOC");
        if (string.IsNullOrEmpty(protoc)) protoc = "protoc";

        var staging = Directory.CreateTempSubdirectory("protobus-codegen-").FullName;
        try
        {
            var staged = Staging.Stage(dirs, staging, custom, customNamespace);
            if (staged.Schemas.Count == 0)
            {
                stderr.WriteLine($"protobus-csharp: no .proto files under {string.Join(", ", dirs)}");
                return 1;
            }
            Directory.CreateDirectory(outDir);
            var set = Path.Combine(staging, "descriptor-set.binpb");
            var psi = new ProcessStartInfo(protoc)
            {
                WorkingDirectory = staging,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (var arg in new[] { "-I", staging, "--csharp_out=" + Path.GetFullPath(outDir), "--descriptor_set_out=" + set, "--include_imports" }
                         .Concat(staged.Schemas).Concat(staged.GeneratedTypes))
                psi.ArgumentList.Add(arg);
            string output;
            int code;
            try
            {
                using var p = Process.Start(psi)!;
                var err = p.StandardError.ReadToEndAsync();
                output = p.StandardOutput.ReadToEnd() + err.Result;
                p.WaitForExit();
                code = p.ExitCode;
            }
            catch (System.ComponentModel.Win32Exception e)
            {
                throw new IOException($"cannot run protoc ({protoc}): {e.Message}");
            }
            if (output.Trim().Length > 0) stderr.Write(output);
            if (code != 0)
            {
                stderr.WriteLine($"protobus-csharp: protoc failed (exit {code})");
                return 1;
            }
            var descriptors = FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(set));
            var built = Build(descriptors.File);
            var written = WriteAll(staged.Schemas.Select(n => built[n]), outDir);
            if (descriptorOut != null)
            {
                var parent = Path.GetDirectoryName(descriptorOut);
                if (!string.IsNullOrEmpty(parent)) Directory.CreateDirectory(parent);
                File.WriteAllBytes(descriptorOut, descriptors.ToByteArray());
            }
            stdout.WriteLine($"protobus-csharp: {staged.Schemas.Count} schema(s), {written} service class(es) written to {outDir}");
            return 0;
        }
        finally
        {
            try
            {
                Directory.Delete(staging, true);
            }
            catch (IOException)
            {
                // Best effort: a temp directory.
            }
        }
    }

    /// <summary>Write every schema's service classes; two services generating one file is refused.</summary>
    private static int WriteAll(IEnumerable<FileDescriptor> files, string outDir)
    {
        var outputs = new Dictionary<string, string>();
        foreach (var f in files)
        foreach (var o in Generator.Generate(f))
        {
            if (outputs.TryGetValue(o.Path, out var other))
                throw new IOException($"{o.Path} would be generated by both {other} and {f.Name}: rename one of the services");
            outputs[o.Path] = f.Name;
            File.WriteAllText(Path.Combine(outDir, o.Path), o.Content, new UTF8Encoding(false));
        }
        return outputs.Count;
    }

    /// <summary>Descriptors for a dependency-ordered list of files, as protoc emits them.</summary>
    internal static Dictionary<string, FileDescriptor> Build(IEnumerable<FileDescriptorProto> files)
    {
        var list = files.ToList();
        IReadOnlyList<FileDescriptor> built;
        try
        {
            built = FileDescriptor.BuildFromByteStrings(list.Select(f => f.ToByteString()));
        }
        catch (ArgumentException e)
        {
            throw new IOException("invalid schema: " + e.Message, e);
        }
        return built.ToDictionary(f => f.Name);
    }

    /// <summary>protoc plugin mode.</summary>
    private static int Plugin()
    {
        var request = CodeGeneratorRequest.Parser.ParseFrom(Console.OpenStandardInput());
        var response = new CodeGeneratorResponse
        {
            SupportedFeatures = (ulong)CodeGeneratorResponse.Types.Feature.Proto3Optional,
        };
        try
        {
            var built = Build(request.ProtoFile);
            var seen = new Dictionary<string, string>();
            foreach (var name in request.FileToGenerate)
            foreach (var o in Generator.Generate(built[name]))
            {
                if (seen.TryGetValue(o.Path, out var other))
                    throw new IOException($"{o.Path} would be generated by both {other} and {name}: rename one of the services");
                seen[o.Path] = name;
                response.File.Add(new CodeGeneratorResponse.Types.File { Name = o.Path, Content = o.Content });
            }
        }
        catch (Exception e) when (e is IOException or ArgumentException or KeyNotFoundException)
        {
            response.Error = e.Message;
        }
        using var stdout = Console.OpenStandardOutput();
        response.WriteTo(stdout);
        return 0;
    }
}
