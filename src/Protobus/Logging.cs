using System;
using System.Text;

namespace Protobus;

/// <summary>Logging thresholds, from the most verbose. The initial level comes from <c>LOG_LEVEL</c>.</summary>
public enum LogLevel
{
    Debug = 10,
    Info = 20,
    Warn = 30,
    Error = 40,
    Silent = 100,
}

/// <summary>
/// Where protobus writes its log lines; install one with <see cref="Logger.Set"/>. A sink may be
/// called from any thread, and never sees a line below <see cref="Logger.Level"/>. Adapting
/// Microsoft.Extensions.Logging takes four one-line methods.
/// </summary>
public interface ILogSink
{
    void Debug(string message);
    void Info(string message);
    void Warn(string message);
    void Error(string message);
}

/// <summary>
/// The library's logger: a level threshold in front of a replaceable sink, the console by
/// default. protobus never logs a message body: payloads routinely carry credentials and
/// personal data, so log lines name types, sizes and ids.
/// </summary>
public static class Logger
{
    private static volatile ILogSink sink = new ConsoleSink();
    private static LogLevel level = FromEnv();

    public static LogLevel Level
    {
        get => level;
        set => level = value;
    }

    /// <summary>Replace the sink; null restores the console.</summary>
    public static void Set(ILogSink? newSink) => sink = newSink ?? new ConsoleSink();

    internal static bool Enabled(LogLevel at) => (int)level <= (int)at;

    internal static ILogSink Sink => sink;

    public static void Debug(string message) { if (Enabled(LogLevel.Debug)) sink.Debug(message); }
    public static void Info(string message) { if (Enabled(LogLevel.Info)) sink.Info(message); }
    public static void Warn(string message) { if (Enabled(LogLevel.Warn)) sink.Warn(message); }
    public static void Error(string message) { if (Enabled(LogLevel.Error)) sink.Error(message); }

    private static LogLevel FromEnv() => (Config.Raw("LOG_LEVEL") ?? "").Trim().ToLowerInvariant() switch
    {
        "debug" => LogLevel.Debug,
        "warn" or "warning" => LogLevel.Warn,
        "error" => LogLevel.Error,
        "silent" or "off" or "none" => LogLevel.Silent,
        _ => LogLevel.Info,
    };

    /// <summary>
    /// The URL with its password replaced by <c>***</c>, so it is safe to log. Anything that does
    /// not parse is <c>&lt;redacted&gt;</c>: it may still be a credential.
    /// </summary>
    public static string RedactUrl(string? url)
    {
        if (string.IsNullOrEmpty(url)) return url ?? "null";
        var scheme = url.IndexOf("://", StringComparison.Ordinal);
        if (scheme <= 0 || url.IndexOf(' ') >= 0) return "<redacted>";
        var rest = url.Substring(scheme + 3);
        var slash = rest.IndexOfAny(new[] { '/', '?' });
        var authority = slash < 0 ? rest : rest.Substring(0, slash);
        var tail = slash < 0 ? "" : rest.Substring(slash);
        var at = authority.LastIndexOf('@');
        if (at < 0) return url;
        var userInfo = authority.Substring(0, at);
        var colon = userInfo.IndexOf(':');
        if (colon < 0) return url;
        return new StringBuilder(url.Substring(0, scheme + 3)).Append(userInfo, 0, colon).Append(":***@")
            .Append(authority.Substring(at + 1)).Append(tail).ToString();
    }

    /// <summary>Info and debug to stdout, warnings and errors to stderr.</summary>
    public sealed class ConsoleSink : ILogSink
    {
        public void Debug(string message) => Console.Out.WriteLine(Stamp("DEBUG", message));
        public void Info(string message) => Console.Out.WriteLine(Stamp("INFO", message));
        public void Warn(string message) => Console.Error.WriteLine(Stamp("WARN", message));
        public void Error(string message) => Console.Error.WriteLine(Stamp("ERROR", message));

        private static string Stamp(string lvl, string message) =>
            $"{DateTime.UtcNow:O} {lvl} protobus: {message}";
    }
}
