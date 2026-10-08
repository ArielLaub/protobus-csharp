using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Chat;
using Protobus;

namespace Examples;

/// <summary>
/// Server-streaming with cancellation, in the shape a chat UI needs: a service streams tokens like
/// a language model, and the caller stops it three ways.
///
/// After each run the demo asks the SERVER how much it generated. Cancellation that only stopped
/// the reader would show the full count; here the producer stops, because the caller's cancel
/// reaches it as its CancellationToken firing.
/// </summary>
public static class TokenstreamExample
{
    private static List<string> Completion(string prompt)
    {
        var body = $"Answering \"{prompt}\". " + string.Concat(Enumerable.Repeat(
            "Streaming responses arrive one token at a time, which is what lets a chat interface render text as it is "
            + "produced rather than waiting for a whole reply. That same property is what makes stopping useful: when "
            + "the reader has seen enough, every token after that point is wasted work on the server. ", 3));
        return body.Trim().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Select(w => w + " ").ToList();
    }

    /// <summary>Streams a long canned completion, one word at a time.</summary>
    public sealed class Assistant : AssistantProtobus.Base
    {
        private readonly object sync = new();
        private StatsResponse stats = new();

        public Assistant(Context context, MessageServiceOptions? options = null) : base(context, options) { }

        public override async IAsyncEnumerable<Token> Generate(GenerateRequest request, CallContext context)
        {
            var words = Completion(request.Prompt);
            var delay = TimeSpan.FromMilliseconds(request.TokenDelayMs > 0 ? request.TokenDelayMs : 60);
            lock (sync) stats = new StatsResponse();
            for (var i = 0; i < words.Count; i++)
            {
                // The token fires when the caller cancels: it left the loop, cancelled its own
                // token, or went idle. This is where a real service would abort its upstream model
                // call, and the point of the demo: stopping saves the work, not just the reading.
                try
                {
                    await Task.Delay(delay, context.CancellationToken);
                }
                catch (OperationCanceledException)
                {
                    lock (sync) stats.StoppedEarly = true;
                    yield break;
                }
                yield return new Token { Index = i, Text = words[i] };
                lock (sync) stats.TokensGenerated = i + 1;
            }
        }

        public override Task<StatsResponse> Stats(StatsRequest request, CallContext context)
        {
            lock (sync) return Task.FromResult(stats.Clone());
        }
    }

    private static void Header(string title)
    {
        var rule = new string('=', 64);
        Console.WriteLine($"\n{rule}\n{title}\n{rule}");
    }

    private static async Task ReportAsync(AssistantProtobus.Proxy client)
    {
        await Task.Delay(300); // let the cancellation travel
        var stats = await client.StatsAsync(new StatsRequest());
        Console.WriteLine($"  server generated {stats.TokensGenerated} tokens; stopped early: {stats.StoppedEarly}");
    }

    /// <summary>Stop lives outside the loop (a UI handler, another task) and takes effect at once.</summary>
    private static async Task StopButtonAsync(AssistantProtobus.Proxy client)
    {
        Header("1. Stop button (a CancellationToken)");
        using var stop = new CancellationTokenSource();
        var button = Task.Run(async () =>
        {
            await Task.Delay(900);
            Console.WriteLine("\n  [user pressed Stop]");
            stop.Cancel();
        });
        Console.Write("  ");
        // A cancelled stream ends rather than raising.
        await foreach (var token in client.Generate(new GenerateRequest { Prompt = "why does streaming matter?", TokenDelayMs = 60 },
                           cancellationToken: stop.Token))
            Console.Write(token.Text);
        await button;
        await ReportAsync(client);
    }

    /// <summary>The decision is made inside the loop: leaving it cancels the stream.</summary>
    private static async Task BreakOutAsync(AssistantProtobus.Proxy client)
    {
        Header("2. break out of the loop");
        Console.Write("  ");
        var printed = 0;
        await foreach (var token in client.Generate(new GenerateRequest { Prompt = "only the first few words", TokenDelayMs = 60 }))
        {
            Console.Write(token.Text);
            if (++printed == 8) break;
        }
        Console.WriteLine("\n  [consumer stopped reading]");
        await ReportAsync(client);
    }

    /// <summary>The control, where nothing cancels.</summary>
    private static async Task RunToCompletionAsync(AssistantProtobus.Proxy client)
    {
        Header("3. no cancellation");
        var received = 0;
        await foreach (var _ in client.Generate(new GenerateRequest { Prompt = "short answer", TokenDelayMs = 1 })) received++;
        Console.WriteLine($"  received {received} tokens");
        await ReportAsync(client);
    }

    public static async Task RunAsync()
    {
        await using var context = new Context();
        await context.InitAsync(Program.AmqpUrl);
        // A streaming handler holds its prefetch slot for the life of its stream, so concurrency
        // is how many callers are served at once.
        await using var assistant = new Assistant(context, new MessageServiceOptions { MaxConcurrent = 8 });
        await assistant.InitAsync();

        var client = new AssistantProtobus.Proxy(context);
        client.Init();
        await StopButtonAsync(client);
        await BreakOutAsync(client);
        await RunToCompletionAsync(client);
    }
}
