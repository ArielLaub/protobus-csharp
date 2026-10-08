using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Protobus.Codegen;
using Xunit;

namespace Protobus.Tests;

public class StagingTests
{
    private const string Schema = "syntax = \"proto3\";\npackage billing;\nmessage Invoice {\n  bigint amount = 1;\n"
        + "  map<string, timestamp> due = 2;\n  int64 timestamp = 3; // a field named like the type\n}\n";

    [Fact]
    public void TheCustomTypeImportIsAppended()
    {
        var staged = Staging.Stage(Schema, Array.Empty<Staging.CustomType>());
        Assert.StartsWith(Schema, staged); // the original text is kept as is, so line numbers match
        Assert.Contains("import \"protobus/types.proto\";", staged);
    }

    [Fact]
    public void NamesInCommentsAndStringsDoNotCount()
    {
        var schema = "syntax = \"proto3\";\n// bigint x = 1;\n/* timestamp y = 2; */\n"
            + "message M { string s = 1 [json_name = \"bigint z = 3\"]; int64 timestamp = 2; }\n";
        Assert.Equal(schema, Staging.Stage(schema, Array.Empty<Staging.CustomType>()));
    }

    [Fact]
    public void ASchemaThatDeclaresOrImportsTheTypesIsLeftAlone()
    {
        var declares = "syntax = \"proto3\";\nmessage bigint { optional bytes value = 1; }\nmessage M { bigint a = 1; }\n";
        Assert.Equal(declares, Staging.Stage(declares, Array.Empty<Staging.CustomType>()));
        var imports = "syntax = \"proto3\";\nimport \"protobus/types.proto\";\nmessage M { bigint a = 1; }\n";
        Assert.Equal(imports, Staging.Stage(imports, Array.Empty<Staging.CustomType>()));
    }

    [Fact]
    public void TypePositionsAreRecognised()
    {
        var code = Staging.Code("message M { repeated bigint a = 1; map<string,bigint> b = 2; }\nservice S { rpc f(timestamp) returns (Empty); }");
        Assert.True(Staging.Uses(code, "bigint"));
        Assert.True(Staging.Uses(code, "timestamp"));
        Assert.False(Staging.Uses(Staging.Code("message M { my.bigint a = 1; }"), "bigint"));
        Assert.True(Staging.Uses(Staging.Code("message M { .bigint a = 1; }"), "bigint"));
    }

    [Fact]
    public void CustomTypesAreDeclaredAndImported()
    {
        var uuid = Staging.CustomType.Parse("uuid:bytes");
        var staged = Staging.Stage("syntax = \"proto3\";\nmessage M { uuid id = 1; }\n", new[] { uuid });
        Assert.Contains("import \"protobus/custom/uuid.proto\";", staged);
        Assert.DoesNotContain("protobus/types.proto", staged);
        var schema = Staging.CustomTypeSchema(uuid, "Acme.Types");
        Assert.Contains("message uuid {\n  optional bytes value = 1;\n}", schema);
        Assert.Contains("option csharp_namespace = \"Acme.Types\";", schema);
        Assert.Throws<ArgumentException>(() => Staging.CustomType.Parse("uuid:float"));
        Assert.Throws<ArgumentException>(() => Staging.CustomType.Parse("bigint:bytes"));
        Assert.Throws<ArgumentException>(() => Staging.CustomType.Parse("bad-name:bytes"));
    }

    [Fact]
    public void ACommentMarkerInsideAStringIsNotAComment()
    {
        var schema = "syntax = \"proto3\";\noption go_package = \"example.com/x//y\"; import \"protobus/types.proto\";\n"
            + "message M { bigint a = 1; }\n";
        Assert.Equal(schema, Staging.Stage(schema, Array.Empty<Staging.CustomType>()));
    }

    [Fact]
    public void ThePascalCaseMatchesProtoc()
    {
        Assert.Equal("FooBar", Names.Pascal("foo_bar"));
        Assert.Equal("Add", Names.Pascal("add"));
        Assert.Equal("FetchAsync", Names.Pascal("fetchAsync"));
        Assert.Equal("V2Api", Names.Pascal("v2_api"));
        Assert.Equal("V2Api", Names.Pascal("v2api"));
    }
}

public sealed class GeneratorTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("protobus-gen-test-").FullName;

    public void Dispose() => Directory.Delete(dir, true);

    private static string Protoc()
    {
        var p = typeof(GeneratorTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>().FirstOrDefault(a => a.Key == "Protoc")?.Value;
        Assert.SkipWhen(string.IsNullOrEmpty(p) || !File.Exists(p), "no protoc for the generator test");
        return p!;
    }

    private int Run(params string[] args)
    {
        var log = new StringWriter();
        var code = Program.Run(args, log, log);
        Output = log.ToString();
        return code;
    }

    private string Output = "";

    [Fact]
    public void GeneratesTheServiceClassesAndADescriptorSet()
    {
        var proto = Directory.CreateDirectory(Path.Combine(dir, "proto", "shop")).FullName;
        File.WriteAllText(Path.Combine(proto, "orders.proto"), "syntax = \"proto3\";\npackage shop.orders;\nservice Orders {\n"
            + "  rpc place(Order) returns (Receipt);\n  rpc watch(Order) returns (stream Receipt);\n  rpc init(Order) returns (Receipt);\n}\n"
            + "message Order { bigint total = 1; }\nmessage Receipt { timestamp at = 1; uuid id = 2; }\n");
        var o = Path.Combine(dir, "out");
        var set = Path.Combine(dir, "res", "schemas.binpb");
        Assert.True(0 == Run("generate", "--proto-dir", Path.Combine(dir, "proto"), "--out", o, "--descriptor-out", set,
            "--custom-type", "uuid:bytes", "--protoc", Protoc()), Output);
        var svc = File.ReadAllText(Path.Combine(o, "OrdersProtobus.cs"));
        Assert.Contains("namespace Shop.Orders;", svc);
        Assert.Contains("public const string ServiceFullName = \"shop.orders.Orders\";", svc);
        Assert.Contains("Task<global::Shop.Orders.Receipt> Place(global::Shop.Orders.Order request", svc);
        Assert.Contains("IAsyncEnumerable<global::Shop.Orders.Receipt> Watch(global::Shop.Orders.Order request", svc);
        // Init is a ServiceProxy member: the rpc's methods step aside.
        Assert.Contains("RegisterUnary<global::Shop.Orders.Order, global::Shop.Orders.Receipt>(\"init\", global::Shop.Orders.Order.Parser, Init_);", svc);
        Assert.Contains("InitAsync_(", svc);
        Assert.Contains("PlaceAsync(", svc);
        Assert.True(File.Exists(Path.Combine(o, "Orders.cs")), "protoc's message classes");
        Assert.Contains("public sealed partial class uuid", File.ReadAllText(Path.Combine(o, "Uuid.cs")));
        Assert.False(File.Exists(Path.Combine(o, "Types.cs")), "the built-in types ship with the runtime");
        Assert.True(new FileInfo(set).Length > 0);
        // The originals are untouched.
        Assert.DoesNotContain("import", File.ReadAllText(Path.Combine(proto, "orders.proto")));
    }

    [Fact]
    public void ASchemaErrorFailsWithProtocsMessage()
    {
        var proto = Directory.CreateDirectory(Path.Combine(dir, "proto")).FullName;
        File.WriteAllText(Path.Combine(proto, "bad.proto"), "syntax = \"proto3\";\nmessage M { nope x = 1; }\n");
        Assert.Equal(1, Run("generate", "--proto-dir", proto, "--out", Path.Combine(dir, "out"), "--protoc", Protoc()));
        Assert.Contains("bad.proto:2", Output);
    }

    [Fact]
    public void TwoServicesGeneratingOneFileAreRefused()
    {
        var proto = Directory.CreateDirectory(Path.Combine(dir, "proto")).FullName;
        File.WriteAllText(Path.Combine(proto, "a.proto"), "syntax = \"proto3\";\npackage a;\nmessage N {}\nservice Svc { rpc f(N) returns (N); }\n");
        File.WriteAllText(Path.Combine(proto, "b.proto"), "syntax = \"proto3\";\npackage b;\nmessage N {}\nservice Svc { rpc f(N) returns (N); }\n");
        var e = Assert.Throws<IOException>(() => Run("generate", "--proto-dir", proto, "--out", Path.Combine(dir, "out"), "--protoc", Protoc()));
        Assert.Contains("SvcProtobus.cs", e.Message);
    }

    [Fact]
    public void UsageErrorsAreReported()
    {
        Assert.Throws<ArgumentException>(() => Run("frobnicate"));
        Assert.Throws<ArgumentException>(() => Run("generate", "--out", "x"));
        Assert.Throws<ArgumentException>(() => Run("generate", "--proto-dir"));
    }
}
