// The protobus examples.
//
//   docker compose up -d --wait
//   export AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
//   dotnet run --project examples -- calculator [server|client|both]
//   dotnet run --project examples -- tokenstream
//   dotnet run --project examples -- combat
using System;
using System.Threading.Tasks;

namespace Examples;

public static class Program
{
    internal static string AmqpUrl => Environment.GetEnvironmentVariable("AMQP_URL") is { Length: > 0 } u
        ? u
        : "amqp://guest:guest@127.0.0.1:25672/";

    public static async Task<int> Main(string[] args)
    {
        switch (args.Length > 0 ? args[0] : "")
        {
            case "calculator":
                return await CalculatorExample.RunAsync(args.Length > 1 ? args[1] : "both");
            case "tokenstream":
                await TokenstreamExample.RunAsync();
                return 0;
            case "combat":
                return await CombatExample.RunAsync();
            default:
                Console.Error.WriteLine("usage: protobus-examples calculator [server|client|both] | tokenstream | combat");
                return 2;
        }
    }
}
