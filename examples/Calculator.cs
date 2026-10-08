using System;
using System.Threading.Tasks;
using Calculator;
using Protobus;

namespace Examples;

/// <summary>
/// The protobus getting-started example: a service, a client, and an event, in one program.
/// The schema (proto/Calculator.proto) is an ordinary protobus schema, shared as-is with
/// TypeScript, Python, Go, C++ and Java services.
/// </summary>
public static class CalculatorExample
{
    /// <summary>Implements Calculator.Service. An rpc left unimplemented answers PROTOCOL_ERROR.</summary>
    public sealed class CalculatorService : ServiceProtobus.Base
    {
        public CalculatorService(Context context, MessageServiceOptions? options = null) : base(context, options) { }

        public override async Task<AddResponse> Add(AddRequest request, CallContext context)
        {
            await AnnounceAsync("add");
            return new AddResponse { Result = request.A + request.B };
        }

        public override async Task<DivideResponse> Divide(DivideRequest request, CallContext context)
        {
            // A HandledError is an answer, not a failure: the caller gets it at once and it is
            // never retried. Anything else thrown is retried.
            if (request.Divisor == 0) throw new HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
            await AnnounceAsync("divide");
            return new DivideResponse { Quotient = request.Dividend / request.Divisor };
        }

        // Events fan out: every subscribing service gets one.
        private Task AnnounceAsync(string operation) =>
            PublishEventAsync(new Calculated { Operation = operation, At = CustomTypes.Timestamp(DateTimeOffset.UtcNow) });
    }

    public static async Task CallAsync(Context context)
    {
        var calculator = new ServiceProtobus.Proxy(context);
        calculator.Init();

        var sum = await calculator.AddAsync(new AddRequest { A = 20, B = 22 });
        Console.WriteLine($"20 + 22 = {sum.Result}");

        var quotient = await calculator.DivideAsync(new DivideRequest { Dividend = 1, Divisor = 4 },
            new CallOptions { Actor = "example-client" });
        Console.WriteLine($"1 / 4 = {quotient.Quotient}");

        try
        {
            await calculator.DivideAsync(new DivideRequest { Dividend = 1, Divisor = 0 });
        }
        catch (RemoteError e)
        {
            Console.WriteLine($"1 / 0 -> {e.Message} ({e.Code})");
        }
    }

    public static async Task<int> RunAsync(string mode)
    {
        var context = new Context();
        await context.InitAsync(Program.AmqpUrl);

        if (mode == "client")
        {
            await CallAsync(context);
            await context.DisposeAsync();
            return 0;
        }

        var service = await RunnableService.StartAsync(context, (c, o) => new CalculatorService(c, o),
            new MessageServiceOptions { MaxConcurrent = 8 });
        // Subscribers receive typed events, on the default topic EVENT.Calculator.Calculated.
        await service.SubscribeEventAsync<Calculated>((e, type, topic) =>
        {
            Console.WriteLine($"event: {e.Operation} at {CustomTypes.ToDateTimeOffset(e.At):O}");
            return Task.CompletedTask;
        });

        if (mode == "both")
        {
            await CallAsync(context);
            // Let the events arrive before shutting down.
            await Task.Delay(300);
            RunnableService.RequestShutdown();
        }
        else
        {
            Console.WriteLine("calculator service running; Ctrl-C to stop");
        }
        // Returns once SIGINT/SIGTERM (or RequestShutdown) has shut the service down gracefully.
        return await RunnableService.WaitForShutdownAsync();
    }
}
