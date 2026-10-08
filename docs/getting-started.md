# Getting Started

From an empty directory to a service, a client, an event and a unit test. You
need the .NET 8 SDK (or newer) and Docker for RabbitMQ.

## 1. A broker

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

## 2. A project

```bash
dotnet new console -o calculator && cd calculator
dotnet add package Protobus
dotnet add package Protobus.Tools
mkdir schemas
```

Point the generator at the schemas, in `calculator.csproj`:

```xml
<ItemGroup>
  <ProtobusSchemas Include="schemas" />
</ItemGroup>
```

## 3. The contract

`schemas/Calculator.proto`:

```protobuf
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest { int32 a = 1; int32 b = 2; }
message AddResponse { int32 result = 1; }
message DivideRequest { double dividend = 1; double divisor = 2; }
message DivideResponse { double quotient = 1; }

// An event: published by the service, received by any subscriber.
message Calculated {
  string operation = 1;
  timestamp at = 2;
}
```

`timestamp` is a protobus custom type; it needs no import. `dotnet build` now
generates `Calculator.ServiceProtobus`, with a `Base` to implement and a
`Proxy` to call.

## 4. The service

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public class CalculatorService : ServiceProtobus.Base
{
    public CalculatorService(Context context, MessageServiceOptions? options = null) : base(context, options) { }

    public override async Task<AddResponse> Add(AddRequest request, CallContext context)
    {
        await PublishEventAsync(new Calculated { Operation = "add", At = CustomTypes.Timestamp(DateTimeOffset.UtcNow) });
        return new AddResponse { Result = request.A + request.B };
    }

    public override Task<DivideResponse> Divide(DivideRequest request, CallContext context)
    {
        if (request.Divisor == 0) throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
        return Task.FromResult(new DivideResponse { Quotient = request.Dividend / request.Divisor });
    }
}
```

What a handler throws decides what happens: a `HandledError` is answered at
once and never retried; anything else is retried and, after three retries,
dead-lettered to `Calculator.Service.DLQ`. See [Errors](errors.md).

## 5. Run it

<!-- doc-check: compile -->
```csharp
using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

public static class ServeAndCall
{
    public static async Task<int> RunAsync(string[] args)
    {
        var context = new Context();
        await context.InitAsync(Environment.GetEnvironmentVariable("AMQP_URL")!);

        if (args.Length > 0 && args[0] == "client")
        {
            var calculator = new ServiceProtobus.Proxy(context);
            calculator.Init();
            Console.WriteLine((await calculator.AddAsync(new AddRequest { A = 2, B = 3 })).Result);
            try
            {
                await calculator.DivideAsync(new DivideRequest { Dividend = 1, Divisor = 0 });
            }
            catch (RemoteError e)
            {
                Console.WriteLine($"{e.Code}: {e.Message}"); // DIVISION_BY_ZERO: cannot divide by zero
            }
            await context.DisposeAsync();
            return 0;
        }

        var service = await RunnableService.StartAsync(context, c => new CalculatorService(c));
        await service.SubscribeEventAsync<Calculated>((e, type, topic) =>
        {
            Console.WriteLine($"{e.Operation} at {CustomTypes.ToDateTimeOffset(e.At):O}");
            return Task.CompletedTask;
        });
        // Until SIGINT or SIGTERM: then stop intake, drain, clean up and close.
        return await RunnableService.WaitForShutdownAsync();
    }
}
```

Call it from `Program.cs` (`return await ServeAndCall.RunAsync(args);`), then:

```bash
dotnet run &              # the service
dotnet run -- client      # 5, then DIVISION_BY_ZERO: cannot divide by zero
```

Run the service twice and the broker shares the requests between the two
processes.

## 6. A unit test

`MemoryBroker` is a broker in memory, so a test needs no RabbitMQ:

<!-- doc-check: compile -->
```csharp
using System.Threading.Tasks;
using Calculator;
using Protobus;
using Protobus.Testing;

public static class CalculatorTest
{
    public static async Task AddsAsync()
    {
        await using var broker = new MemoryBroker();
        await using var context = new Context(new ContextOptions { Transport = broker });
        await context.InitAsync("amqp://memory/");
        await using var service = new CalculatorService(context);
        await service.InitAsync();

        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();
        var sum = await calculator.AddAsync(new AddRequest { A = 2, B = 3 });
        if (sum.Result != 5) throw new System.Exception("2 + 3 should be 5");
    }
}
```

See [Testing](testing.md) for fault injection and inspection.

## Next

- [Services](services.md): options, concurrency, instance names.
- [Streaming](streaming.md): `returns (stream T)`.
- [Compatibility](compatibility.md): the same schema, served or called from
  TypeScript, Python, Go, C++ or Java.
