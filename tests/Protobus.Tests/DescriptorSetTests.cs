using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Pbtest;
using Protobus.Codegen;
using Xunit;

namespace Protobus.Tests;

/// <summary>Schemas known only at runtime, from serialized descriptor sets.</summary>
public sealed class DescriptorSetTests : MemoryBus, IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("protobus-dset-").FullName;

    public void Dispose() => Directory.Delete(dir, true);

    private const string Schema = "syntax = \"proto3\";\npackage dyn;\n"
        + "service Echo { rpc say(Msg) returns (Msg); rpc tick(Msg) returns (stream Msg); }\n"
        + "message Msg { string text = 1; bigint n = 2; }\n";

    private static string Protoc()
    {
        var p = typeof(DescriptorSetTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(a => a.Key == "Protoc")?.Value;
        Assert.SkipWhen(string.IsNullOrEmpty(p) || !File.Exists(p), "no protoc");
        return p!;
    }

    /// <summary>The descriptor set of dyn.proto, as the generator writes it (imports included).</summary>
    private string WriteSet()
    {
        var proto = Directory.CreateDirectory(Path.Combine(dir, "proto")).FullName;
        File.WriteAllText(Path.Combine(proto, "dyn.proto"), Schema);
        var set = Path.Combine(dir, "sets", "dyn.binpb");
        var log = new StringWriter();
        Assert.True(0 == Codegen.Program.Run(new[] { "generate", "--proto-dir", proto, "--out", Path.Combine(dir, "gen"),
            "--descriptor-out", set, "--protoc", Protoc() }, log, log), log.ToString());
        return set;
    }

    private sealed class EchoService : MessageService
    {
        public EchoService(Context c) : base(c)
        {
            RegisterMethod("say", (payload, call) => Task.FromResult(payload));
        }

        public override string ServiceName => "dyn.Echo";
    }

    [Fact]
    public async Task AServiceKnownOnlyFromADescriptorSetIsServedAndCalled()
    {
        Ctx.Factory.LoadDescriptorSet(File.ReadAllBytes(WriteSet()));
        Assert.True(Ctx.Factory.HasService("dyn.Echo"));
        Assert.True(Ctx.Factory.IsStreamingMethod("dyn.Echo.tick"));
        await using var s = new EchoService(Ctx);
        await s.InitAsync();
        var p = new ServiceProxy(Ctx, "dyn.Echo");
        p.Init();
        var payload = new byte[] { 0x0a, 0x02, (byte)'h', (byte)'i' };
        Assert.Equal(payload, await p.CallRawAsync("say", payload));
    }

    [Fact]
    public void ASetWithoutTheBuiltInTypesStillLoads()
    {
        // protoc without --include_imports: protobus/types.proto is always known.
        var proto = Directory.CreateDirectory(Path.Combine(dir, "bare", "protobus")).FullName;
        File.WriteAllText(Path.Combine(proto, "types.proto"), Staging.TypesSchema());
        File.WriteAllText(Path.Combine(dir, "bare", "dyn.proto"), Schema.Replace("package dyn;", "package dyn;\nimport \"protobus/types.proto\";"));
        var set = Path.Combine(dir, "bare.binpb");
        var psi = new System.Diagnostics.ProcessStartInfo(Protoc()) { WorkingDirectory = Path.Combine(dir, "bare") };
        foreach (var a in new[] { "-I", ".", "--descriptor_set_out=" + set, "dyn.proto" }) psi.ArgumentList.Add(a);
        System.Diagnostics.Process.Start(psi)!.WaitForExit();
        Assert.DoesNotContain(FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(set)).File, f => f.Name == "protobus/types.proto");
        Ctx.Factory.LoadDescriptorSet(File.ReadAllBytes(set));
        Assert.Equal("bigint", Ctx.Factory.Type("dyn.Msg").FindFieldByName("n").MessageType.FullName);
    }

    [Fact]
    public void AMissingImportIsNamed()
    {
        var file = new FileDescriptorProto { Name = "orphan.proto", Package = "o", Syntax = "proto3" };
        file.Dependency.Add("nowhere/else.proto");
        var set = new FileDescriptorSet();
        set.File.Add(file);
        var e = Assert.Throws<SchemaError>(() => Ctx.Factory.LoadDescriptorSet(set.ToByteArray()));
        Assert.Contains("nowhere/else.proto", e.Message);
        Assert.Contains("--include_imports", e.Message);
    }

    [Fact]
    public void BytesThatAreNotADescriptorSetAreRefused() =>
        Assert.Throws<SchemaError>(() => Ctx.Factory.LoadDescriptorSet(new byte[] { 0xff, 0xff, 0xff }));

    [Fact]
    public void ASchemaAlreadyCompiledInIsLeftAsItIs()
    {
        Ctx.Factory.Register(PbtestReflection.Descriptor);
        var set = new FileDescriptorSet();
        set.File.Add(FileDescriptorProto.Parser.ParseFrom(Protobus.Types.TypesReflection.Descriptor.SerializedData));
        set.File.Add(FileDescriptorProto.Parser.ParseFrom(PbtestReflection.Descriptor.SerializedData));
        Ctx.Factory.LoadDescriptorSet(set.ToByteArray());
        Assert.Same(PbtestReflection.Descriptor, Ctx.Factory.Service("pbtest.Calc").File);
    }

    [Fact]
    public async Task ContextInitLoadsEverySetUnderItsLocations()
    {
        var set = WriteSet();
        File.WriteAllText(Path.Combine(Path.GetDirectoryName(set)!, "notes.txt"), "not a set");
        await using var c = new Context(new ContextOptions { Transport = Broker });
        await c.InitAsync("amqp://memory/", new[] { Path.GetDirectoryName(set)! });
        Assert.True(c.Factory.HasService("dyn.Echo"));
        await using var missing = new Context(new ContextOptions { Transport = Broker });
        await Assert.ThrowsAsync<SchemaError>(() => missing.InitAsync("amqp://memory/", new[] { Path.Combine(dir, "nope") }));
    }
}
