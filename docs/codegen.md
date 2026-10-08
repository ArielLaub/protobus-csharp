# Code generation

protobus schemas are ordinary `.proto` files shared by every language. The
generator compiles them with protoc's C# generator and adds, per service, a
`<Service>Protobus` class:

| | |
|---|---|
| `ServiceFullName` | the service's name on the bus, `Calculator.Service` |
| `Descriptor` | the schema's `FileDescriptor` |
| `Base` | the class a service extends: a virtual method per rpc, registered with the dispatcher |
| `Proxy` | a typed client: `<Rpc>Async` per unary rpc, `<Rpc>` returning `IAsyncEnumerable<T>` per streaming one |

Method names are the rpc's name PascalCased, as protoc names fields (`add` →
`Add`, `get_user` → `GetUser`). A name that would collide, with a member of the
base classes (`Init`, `ServiceName`, ...), another rpc, or another rpc's
`Async` method, gets a trailing underscore: `rpc init` is served by `Init_`
and called with `InitAsync_`, and `fetch` beside `fetchAsync` is called with
`FetchAsync_`. Client streaming is not part of the protobus protocol; such an
rpc is skipped.

## Protobus.Tools (MSBuild)

```bash
dotnet add package Protobus.Tools
```

```xml
<ItemGroup>
  <ProtobusSchemas Include="schemas" />
  <ProtobusSchemas Include="../shared/protos" />
</ItemGroup>
```

Before each compile, every `.proto` under the listed directories is generated
into `obj/.../protobus/` and added to the build; it reruns only when a schema
changes. Paths inside a directory are the import paths
(`schemas/billing/invoice.proto` is `billing/invoice.proto`).

| Property | Default | |
|---|---|---|
| `ProtobusCustomTypes` | none | custom types, `uuid:string;money:int64` |
| `ProtobusCustomTypeNamespace` | `Protobus.Custom` | the namespace their classes are generated in |
| `ProtobusProtoc` | the protoc in Grpc.Tools | a protoc of your own |
| `ProtobusOutputDir` | `$(IntermediateOutputPath)protobus/` | where the generated code goes |

protoc comes from the Grpc.Tools package Protobus.Tools depends on. If your
project also uses Grpc.Tools' own `<Protobuf>` items, keep the protobus
schemas out of them: the two would generate the same message classes.

## The CLI

```bash
dotnet tool install --global Protobus.Codegen
protobus-csharp generate --proto-dir schemas --out Generated \
    [--custom-type uuid:string] [--custom-type-namespace Acme.Types] \
    [--descriptor-out schemas.binpb] [--protoc /path/to/protoc]
```

protoc is `--protoc`, then `$PROTOC`, then the one on the PATH. The originals
are never modified: the generator stages copies and compiles those.

## The protoc plugin

Run with no arguments, `protobus-csharp` is a protoc plugin:

```bash
protoc -I schemas --plugin=protoc-gen-protobus=$(which protobus-csharp) \
    --csharp_out=Generated --protobus_out=Generated calculator.proto
```

Schemas compiled this way must import the custom types they use themselves,
`import "protobus/types.proto";`, which ships in the Protobus package
(`content/protos`), because the plugin sees schemas only after protoc has
parsed them. The staging the CLI and Protobus.Tools do adds that import for
you.

## Custom types

`bigint` (unsigned, up to 2^256-1, 32 big-endian bytes) and `timestamp`
(signed milliseconds since the epoch) are built in: use them as field types
without an import. In C# they are `Protobus.Types.bigint` and
`Protobus.Types.timestamp`, converted with `CustomTypes`:

<!-- doc-check: compile -->
```csharp
using System;
using System.Numerics;
using Protobus;

public static class CustomTypeUse
{
    public static void Convert()
    {
        var amount = CustomTypes.Bigint(BigInteger.Pow(10, 30));
        BigInteger back = CustomTypes.ToBigInteger(amount);
        var at = CustomTypes.Timestamp(DateTimeOffset.UtcNow);
        DateTimeOffset when = CustomTypes.ToDateTimeOffset(at);
        Console.WriteLine($"{back} {when:O}");
    }
}
```

A value no port can carry (a negative or oversized `bigint`, a `timestamp`
outside ±8.64e15 ms) is refused on the way out and answered `PROTOCOL_ERROR`
on the way in.

Your own custom types are declared to the generator as `NAME:WIRE`, with
`WIRE` one of `bytes`, `int64`, `uint64`, `string`, `int32`, `uint32` or
`double`: each becomes `message NAME { optional WIRE value = 1; }` at the root,
like the built-ins, so schemas use it without an import. Every port that
shares the schema must declare it the same way.
