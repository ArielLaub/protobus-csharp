using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Interop;
using Protobus.Amqp;
using Xunit;
using Headers = Protobus.Internal.Headers;

namespace Protobus.CrossLang.Tests;

/// <summary>
/// The cross-language suite: protobus-csharp against the TypeScript, Python, Go, C++ and Java
/// ports' real libraries over a real broker, in both directions:
/// <list type="bullet">
///   <item>the C# client against a C#, TypeScript, Python, Go, C++ and Java server;</item>
///   <item>the TypeScript, Python, Go, C++ and Java clients against a C# server;</item>
///   <item>replicas of one service in all six languages sharing a queue and its retry ladder.</item>
/// </list>
/// It needs PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL, and the sibling checkouts:
/// PROTOBUS_TS (default ../protobus, built with <c>npm run build-ts</c>), PROTOBUS_PY (default
/// ../protobus-py, with a venv/), PROTOBUS_GO (default ../protobus-go, with Go on the PATH),
/// PROTOBUS_CPP (default ../protobus-cpp, built into build/) and PROTOBUS_JAVA (default
/// ../protobus-java, with JAVA_HOME set). A missing peer skips its tests, unless its variable is
/// set explicitly, as CI does: then its absence is a failure.
/// </summary>
public sealed class CrossLangTests : IAsyncLifetime
{
    private static readonly string Here = Metadata("CrossLangDir");
    private static readonly HttpClient Http = new();
    private static readonly string[] Others = { "ts", "py", "go", "cpp", "java" };

    private readonly List<Process> servers = new();
    private readonly Dictionary<Process, StringBuilder> stderr = new();
    private string vhost = "";
    private string vhostUrl = "";

    private static string Metadata(string key) =>
        typeof(CrossLangTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>().First(a => a.Key == key).Value!;

    // ---- broker ------------------------------------------------------------------------

    private static string AmqpUrl()
    {
        var url = Environment.GetEnvironmentVariable("PROTOBUS_TEST_AMQP_URL");
        Assert.SkipWhen(string.IsNullOrEmpty(url), "PROTOBUS_TEST_AMQP_URL is not set");
        return url!;
    }

    private static Uri Mgmt()
    {
        var url = Environment.GetEnvironmentVariable("PROTOBUS_TEST_MGMT_URL");
        Assert.SkipWhen(string.IsNullOrEmpty(url), "PROTOBUS_TEST_MGMT_URL is not set");
        return new Uri(url!.TrimEnd('/'));
    }

    private static async Task Api(HttpMethod method, string path, string? body = null)
    {
        var @base = Mgmt();
        var info = string.IsNullOrEmpty(@base.UserInfo) ? "guest:guest" : Uri.UnescapeDataString(@base.UserInfo);
        var request = new HttpRequestMessage(method, new Uri($"{@base.Scheme}://{@base.Host}:{@base.Port}{path}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(info)));
        if (body != null) request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        (await Http.SendAsync(request)).EnsureSuccessStatusCode();
    }

    public async ValueTask InitializeAsync()
    {
        var url = AmqpUrl();
        Mgmt();
        if (Environment.GetEnvironmentVariable("PROTOBUS_TEST_LOG") == null) Logger.Level = LogLevel.Silent;
        vhost = "pbcsharp-" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await Api(HttpMethod.Put, "/api/vhosts/" + vhost);
        var user = new Uri(url).UserInfo;
        var name = string.IsNullOrEmpty(user) ? "guest" : Uri.UnescapeDataString(user.Split(':')[0]);
        await Api(HttpMethod.Put, $"/api/permissions/{vhost}/{name}", "{\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}");
        vhostUrl = new UriBuilder(url) { Path = "/" + vhost }.Uri.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var p in servers) await StopAsync(p);
        if (vhost.Length > 0) await Api(HttpMethod.Delete, "/api/vhosts/" + vhost);
        Logger.Level = LogLevel.Info;
    }

    // ---- peers -------------------------------------------------------------------------

    private static string Sibling(string variable, string name) =>
        Environment.GetEnvironmentVariable(variable) is { Length: > 0 } v
            ? v
            : Path.GetFullPath(Path.Combine(Here, "..", "..", name));

    private static bool Configured(string variable) => Environment.GetEnvironmentVariable(variable) is { Length: > 0 };

    private sealed record Command(string File, IReadOnlyList<string> Args, IReadOnlyDictionary<string, string> Env);

    private static readonly object BuildLock = new();
    private static readonly Dictionary<string, (string? Value, string? Failure)> Built = new();

    /// <summary>Run a build step once per process; its output, or why it failed.</summary>
    private static (string? Value, string? Failure) Once(string key, Func<(string?, string?)> build)
    {
        lock (BuildLock)
        {
            if (!Built.TryGetValue(key, out var r)) Built[key] = r = build();
            return r;
        }
    }

    private static (int Code, string Output) Run(string file, IEnumerable<string> args, string dir, IDictionary<string, string>? env = null)
    {
        var psi = new ProcessStartInfo(file) { WorkingDirectory = dir, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        if (env != null) foreach (var (k, v) in env) psi.Environment[k] = v;
        using var p = Process.Start(psi)!;
        var err = p.StandardError.ReadToEndAsync();
        var output = p.StandardOutput.ReadToEnd() + err.Result;
        p.WaitForExit();
        return (p.ExitCode, output);
    }

    private static (string?, string?) BuildGoPeer()
    {
        var go = Sibling("PROTOBUS_GO", "protobus-go");
        if (!File.Exists(Path.Combine(go, "crosslang", "gopeer", "gopeer.go"))) return (null, $"protobus-go not found at {go} (set PROTOBUS_GO)");
        try
        {
            // Built in a scratch copy whose go.mod points at the checkout under test.
            var dir = Directory.CreateTempSubdirectory("pbcsharp-gopeer-").FullName;
            File.Copy(Path.Combine(Here, "peers", "go", "main.go"), Path.Combine(dir, "main.go"));
            File.Copy(Path.Combine(go, "go.sum"), Path.Combine(dir, "go.sum"));
            File.WriteAllText(Path.Combine(dir, "go.mod"), "module protobus-csharp/crosslang/gopeer\n\ngo 1.25\n\n"
                + "require github.com/ArielLaub/protobus-go/v2 v2.0.0\n\n"
                + "replace github.com/ArielLaub/protobus-go/v2 => " + Path.GetFullPath(go) + "\n");
            var output = Path.Combine(dir, "gopeer");
            var (code, log) = Run("go", new[] { "build", "-o", output, "." }, dir, new Dictionary<string, string> { ["GOFLAGS"] = "-mod=mod" });
            return code == 0 && File.Exists(output) ? (output, null) : (null, "could not build the Go peer: " + log);
        }
        catch (Exception e)
        {
            return (null, "could not build the Go peer: " + e.Message);
        }
    }

    private static string? JavaExe() =>
        Environment.GetEnvironmentVariable("JAVA_HOME") is { Length: > 0 } home && File.Exists(Path.Combine(home, "bin", "java"))
            ? Path.Combine(home, "bin", "java")
            : null;

    private static (string?, string?) JavaClasspath()
    {
        var java = Sibling("PROTOBUS_JAVA", "protobus-java");
        if (!File.Exists(Path.Combine(java, "gradlew"))) return (null, $"protobus-java not found at {java} (set PROTOBUS_JAVA)");
        if (JavaExe() == null) return (null, "JAVA_HOME is not set to a JDK (needed for the Java peer)");
        var (code, output) = Run(Path.Combine(java, "gradlew"), new[] { "-q", ":crosslang:classes", ":crosslang:printClasspath" }, java);
        // Gradle may print notices around it: the classpath is the line holding the module's classes.
        var classes = Path.Combine("crosslang", "build", "classes");
        var cp = output.Split('\n').Select(l => l.Trim()).FirstOrDefault(l => l.Contains(classes) && l.Contains(Path.PathSeparator));
        return code == 0 && cp != null ? (cp, null) : (null, "could not build the Java peer: " + output);
    }

    /// <summary>The command for a peer, or a skip (or failure, when configured) saying why it is unavailable.</summary>
    private static Command CommandFor(string lang, string mode)
    {
        string why;
        string variable;
        switch (lang)
        {
            case "csharp":
                return new Command("dotnet", new[] { Metadata("CSharpPeer"), mode }, new Dictionary<string, string>());
            case "ts":
            {
                var ts = Sibling("PROTOBUS_TS", "protobus");
                variable = "PROTOBUS_TS";
                if (File.Exists(Path.Combine(ts, "dist", "lib", "context.js")))
                    return new Command("node", new[] { Path.Combine(Here, "peers", "ts", "peer.js"), mode },
                        new Dictionary<string, string> { ["PROTOBUS_TS"] = ts });
                why = $"TypeScript protobus not built at {ts} (set PROTOBUS_TS)";
                break;
            }
            case "py":
            {
                var py = Sibling("PROTOBUS_PY", "protobus-py");
                variable = "PROTOBUS_PY";
                var interp = Path.Combine(py, "venv", "bin", "python");
                if (File.Exists(interp))
                    return new Command(interp, new[] { Path.Combine(Here, "peers", "py", "peer.py"), mode },
                        new Dictionary<string, string> { ["PYTHONPATH"] = py });
                why = $"protobus-py venv not found at {py} (set PROTOBUS_PY)";
                break;
            }
            case "go":
            {
                variable = "PROTOBUS_GO";
                var (bin, failure) = Once("go", BuildGoPeer);
                if (bin != null) return new Command(bin, new[] { mode }, new Dictionary<string, string>());
                why = failure!;
                break;
            }
            case "cpp":
            {
                variable = "PROTOBUS_CPP";
                var bin = Path.Combine(Sibling("PROTOBUS_CPP", "protobus-cpp"), "build", "crosslang", "cpppeer");
                if (File.Exists(bin)) return new Command(bin, new[] { mode }, new Dictionary<string, string>());
                why = $"protobus-cpp's cpppeer not built at {bin} (set PROTOBUS_CPP)";
                break;
            }
            case "java":
            {
                variable = "PROTOBUS_JAVA";
                var (cp, failure) = Once("java", JavaClasspath);
                if (cp != null)
                    return new Command(JavaExe()!, new[] { "-cp", cp, "io.github.ariellaub.protobus.crosslang.JavaPeer", mode },
                        new Dictionary<string, string>());
                why = failure!;
                break;
            }
            default:
                throw new ArgumentException(lang);
        }
        if (Configured(variable)) Assert.Fail(why);
        Assert.Skip(why);
        return null!;
    }

    private Process Launch(string lang, string mode, string target)
    {
        var c = CommandFor(lang, mode);
        var psi = new ProcessStartInfo(c.File) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in c.Args) psi.ArgumentList.Add(a);
        foreach (var (k, v) in c.Env) psi.Environment[k] = v;
        psi.Environment["PROTOBUS_TEST_AMQP"] = vhostUrl;
        psi.Environment["PROTOBUS_TEST_PROTO_DIR"] = Path.Combine(Here, "proto");
        psi.Environment["PEER_TARGET"] = target;
        var p = Process.Start(psi)!;
        var err = new StringBuilder();
        lock (stderr) stderr[p] = err;
        p.ErrorDataReceived += (_, e) =>
        {
            if (e.Data == null) return;
            lock (err) err.AppendLine(e.Data);
        };
        p.BeginErrorReadLine();
        return p;
    }

    /// <summary>The last lines a peer wrote to stderr, for a failure message.</summary>
    private string StderrOf(Process p)
    {
        StringBuilder err;
        lock (stderr) err = stderr[p];
        lock (err)
        {
            var lines = err.ToString().Split('\n');
            return string.Join("\n", lines.Skip(Math.Max(0, lines.Length - 40)));
        }
    }

    private static async Task StopAsync(Process p)
    {
        try
        {
            if (p.HasExited) return;
            // SIGTERM first, as a process manager would; then the hard way.
            Process.Start("kill", new[] { "-TERM", p.Id.ToString() })?.WaitForExit();
            using var grace = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await p.WaitForExitAsync(grace.Token);
            }
            catch (OperationCanceledException)
            {
                p.Kill(true);
            }
        }
        catch (InvalidOperationException)
        {
            // Already gone.
        }
    }

    /// <summary>Start a peer's server, returning once it has printed READY.</summary>
    private async Task StartServerAsync(string lang)
    {
        var p = Launch(lang, "server", lang);
        servers.Add(p);
        var ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        p.OutputDataReceived += (_, e) =>
        {
            if (e.Data == null) ready.TrySetResult(false);
            else if (e.Data == "READY") ready.TrySetResult(true);
        };
        p.BeginOutputReadLine();
        bool ok;
        try
        {
            ok = await ready.Task.WaitAsync(TimeSpan.FromSeconds(90));
        }
        catch (TimeoutException)
        {
            ok = false;
        }
        if (!ok) Assert.Fail($"{lang} server did not become ready (alive: {!p.HasExited})\n{StderrOf(p)}");
    }

    /// <summary>Run a peer's client scenario against <paramref name="target"/>; every check must pass.</summary>
    private async Task RunClientAsync(string lang, string target)
    {
        var p = Launch(lang, "client", target);
        var passed = 0;
        var done = false;
        var failed = new List<string>();
        while (await p.StandardOutput.ReadLineAsync() is { } l)
        {
            if (l.StartsWith("PASS ")) passed++;
            else if (l.StartsWith("FAIL ")) failed.Add(l.Substring(5));
            else if (l == "DONE") done = true;
        }
        await p.WaitForExitAsync();
        Assert.True(failed.Count == 0, $"{lang} client against {target}: {string.Join("; ", failed)}");
        Assert.True(done, $"{lang} client did not finish (exit {p.ExitCode})\n{StderrOf(p)}");
        Assert.True(passed >= 15, $"{lang} client against {target} passed only {passed} checks");
    }

    // ---- the suites --------------------------------------------------------------------

    [Theory]
    [InlineData("csharp")]
    [InlineData("ts")]
    [InlineData("py")]
    [InlineData("go")]
    [InlineData("cpp")]
    [InlineData("java")]
    public async Task TheCSharpClientAgainstEveryServer(string server)
    {
        await StartServerAsync(server);
        await RunClientAsync("csharp", server);
    }

    [Theory]
    [InlineData("ts")]
    [InlineData("py")]
    [InlineData("go")]
    [InlineData("cpp")]
    [InlineData("java")]
    public async Task EveryClientAgainstACSharpServer(string client)
    {
        CommandFor(client, "client");
        await StartServerAsync("csharp");
        await RunClientAsync(client, "csharp");
    }

    /// <summary>
    /// interop.Flaky in all six languages at once, competing on one queue. Every attempt fails, so
    /// each message climbs the retry ladder across replicas of different languages: the
    /// x-retry-count one writes is read by the others, the queue arguments each declares must be
    /// equivalent to the rest's, and the message must end in the dead-letter queue exactly once,
    /// after exactly three retries.
    /// </summary>
    [Fact]
    public async Task MixedReplicasShareOneRetryLadder()
    {
        var langs = new[] { "csharp" }.Concat(Others).ToArray();
        foreach (var lang in langs) CommandFor(lang, "server");
        foreach (var lang in langs) await StartServerAsync(lang);

        await using var ctx = new Context();
        await ctx.InitAsync(vhostUrl);
        var listener = new EventListener(ctx.Connection);
        await listener.InitAsync(null, "");
        var perMessage = new ConcurrentDictionary<string, int>();
        var byLang = new ConcurrentDictionary<string, int>();
        var total = 0;
        await listener.SubscribeAsync<Attempted>((a, _, _) =>
        {
            perMessage.AddOrUpdate(a.MessageId, 1, (_, n) => n + 1);
            byLang.AddOrUpdate(a.Lang, 1, (_, n) => n + 1);
            System.Threading.Interlocked.Increment(ref total);
            return Task.CompletedTask;
        }, "EVENT.attempted");
        await listener.StartAsync();

        const int messages = 10;
        const int retries = 3;
        var flaky = new FlakyProtobus.Proxy(ctx);
        flaky.Init();
        var calls = Enumerable.Range(0, messages)
            .Select(i => flaky.FailAsync(new FailRequest { Id = i.ToString() }, new CallOptions { MessageId = "flaky-" + i, TimeoutMs = 30000 }))
            .ToList();
        foreach (var c in calls)
        {
            // The caller is answered with the final failure.
            var e = await Assert.ThrowsAsync<RemoteError>(() => c.WaitAsync(TimeSpan.FromSeconds(60)));
            Assert.StartsWith("flaky ", e.Message);
        }
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (total < messages * (retries + 1) && DateTime.UtcNow < deadline) await Task.Delay(20);
        Assert.Equal(messages * (retries + 1), total);
        foreach (var (id, n) in perMessage) Assert.True(n == retries + 1, $"{id}: {n} attempts");
        TestContext.Current.SendDiagnosticMessage("attempts by language: " + string.Join(", ", byLang.Select(kv => $"{kv.Key}={kv.Value}")));
        Assert.True(byLang.Count >= 2, "the ladder never crossed languages; the test proves nothing");

        var ch = await ctx.Connection.OpenChannelAsync();
        var dead = new ConcurrentQueue<Delivery>();
        await ch.ConsumeAsync("interop.Flaky.DLQ", "dlq", true, false, d =>
        {
            dead.Enqueue(d);
            return Task.CompletedTask;
        }, null);
        deadline = DateTime.UtcNow.AddSeconds(30);
        while (dead.Count < messages && DateTime.UtcNow < deadline) await Task.Delay(20);
        Assert.Equal(messages, dead.Count);
        var ids = new HashSet<string?>();
        foreach (var d in dead)
        {
            var h = d.Properties.Headers;
            Assert.Equal(retries, Headers.Integer(Headers.Get(h, "x-retry-count")));
            Assert.Equal("interop.Flaky", Headers.Text(Headers.Get(h, "x-original-queue")));
            Assert.Equal("REQUEST.interop.Flaky.fail", Headers.Text(Headers.Get(h, "x-original-routing-key")));
            var last = Headers.Text(Headers.Get(h, "x-last-error"));
            Assert.False(string.IsNullOrEmpty(last));
            Assert.DoesNotContain("flaky", last);
            ids.Add(d.Properties.MessageId);
        }
        Assert.Equal(messages, ids.Count);
        await listener.CloseAsync();
    }
}
