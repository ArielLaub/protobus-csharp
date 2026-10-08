using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Protobus;

/// <summary>
/// Environment-backed configuration, mirroring every other port's <c>Config</c>.
/// </summary>
/// <remarks>
/// Every getter reads its variable on each call, so a value changed at runtime is
/// picked up. Integer parsing is strict: the trimmed value must be all digits and
/// positive, or the default is kept; a typo never becomes a surprising zero. A
/// boolean must be one of 1/true/yes/on or 0/false/no/off, case-insensitive.
/// <see cref="Set"/> overrides a variable inside the process, for tests and for
/// applications that cannot set their environment. The exchange names are part of
/// the wire protocol: every process on one bus, whatever its language, must agree
/// on them.
/// </remarks>
public static class Config
{
    /// <summary>Named message priorities, matching the other ports. RabbitMQ sorts a message with no priority as 0.</summary>
    public const int PriorityNormal = 0;
    public const int PriorityHigh = 1;
    public const int PriorityControl = 2;

    /// <summary>The <c>maxPriority</c> that gives the three levels above. Keep the range small.</summary>
    public const int RecommendedMaxPriority = 2;

    /// <summary>Headers of the server-streaming wire protocol.</summary>
    public const string HeaderFinal = "x-protobus-final";
    public const string HeaderSeq = "x-protobus-seq";

    private static readonly ConcurrentDictionary<string, string> Overrides = new();
    private static readonly Regex Digits = new("^[0-9]+$", RegexOptions.Compiled);

    /// <summary>Override a variable for this process; null removes the override.</summary>
    public static void Set(string name, string? value)
    {
        if (value == null) Overrides.TryRemove(name, out _);
        else Overrides[name] = value;
    }

    /// <summary>Remove every override.</summary>
    public static void Reset() => Overrides.Clear();

    internal static string? Raw(string name) =>
        Overrides.TryGetValue(name, out var v) ? v : Environment.GetEnvironmentVariable(name);

    internal static long EnvInt(string name, long fallback)
    {
        var raw = Raw(name);
        if (raw == null) return fallback;
        var t = raw.Trim();
        if (t.Length == 0 || !Digits.IsMatch(t)) return fallback;
        // The TypeScript reference refuses anything beyond Number.MAX_SAFE_INTEGER; so does every port.
        if (!long.TryParse(t, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)) return fallback;
        return parsed > 0 && parsed <= 9007199254740991L ? parsed : fallback;
    }

    internal static bool EnvBool(string name, bool fallback)
    {
        var raw = Raw(name);
        if (raw == null || raw.Trim().Length == 0) return fallback;
        switch (raw.Trim().ToLowerInvariant())
        {
            case "1": case "true": case "yes": case "on": return true;
            case "0": case "false": case "no": case "off": return false;
            default: return fallback;
        }
    }

    internal static string EnvString(string name, string fallback)
    {
        var raw = Raw(name);
        return string.IsNullOrEmpty(raw) ? fallback : raw;
    }

    /// <summary>
    /// Send the message of an UNHANDLED service error back to the caller. On by default: a
    /// protobus caller is another of your own services, inside the trust boundary. Turn it off
    /// (<c>PROTOBUS_EXPOSE_INTERNAL_ERRORS=false</c>) for a service whose callers relay errors to
    /// untrusted clients. A HandledError always crosses.
    /// </summary>
    public static bool ExposeInternalErrors => EnvBool("PROTOBUS_EXPOSE_INTERNAL_ERRORS", true);

    /// <summary>RPC requests (topic). <c>BUS_EXCHANGE_NAME</c>, default <c>proto.bus</c>.</summary>
    public static string BusExchangeName => EnvString("BUS_EXCHANGE_NAME", "proto.bus");

    /// <summary>RPC replies (direct). <c>CALLBACKS_EXCHANGE_NAME</c>, default <c>proto.bus.callback</c>.</summary>
    public static string CallbacksExchangeName => EnvString("CALLBACKS_EXCHANGE_NAME", "proto.bus.callback");

    /// <summary>
    /// Stream cancellation (fanout). <c>CANCEL_EXCHANGE_NAME</c>, default <c>proto.bus.cancel</c>.
    /// Fanout, because a cancel has to reach the one replica running that stream.
    /// </summary>
    public static string CancelExchangeName => EnvString("CANCEL_EXCHANGE_NAME", "proto.bus.cancel");

    /// <summary>Events (topic). <c>EVENTS_EXCHANGE_NAME</c>, default <c>proto.bus.events</c>.</summary>
    public static string EventsExchangeName => EnvString("EVENTS_EXCHANGE_NAME", "proto.bus.events");

    /// <summary>One attempt at a unary request. <c>MESSAGE_PROCESSING_TIMEOUT</c>, default 600000 ms.</summary>
    public static long MessageProcessingTimeout => EnvInt("MESSAGE_PROCESSING_TIMEOUT", 600000);

    /// <summary>How long a unary caller waits. <c>RPC_CALL_TIMEOUT_MS</c>, default 600000 ms.</summary>
    public static long RpcCallTimeoutMs => EnvInt("RPC_CALL_TIMEOUT_MS", 600000);

    /// <summary>The longest gap between stream chunks. <c>STREAM_IDLE_TIMEOUT_MS</c>, default 60000 ms.</summary>
    public static long StreamIdleTimeoutMs => EnvInt("STREAM_IDLE_TIMEOUT_MS", 60000);

    /// <summary>Prefetch for late-ack consumers that set none. <c>DEFAULT_PREFETCH</c>, default 1.</summary>
    public static long DefaultPrefetch => EnvInt("DEFAULT_PREFETCH", 1);

    /// <summary>
    /// How long a publish waits for its confirm. Expiry is AMBIGUOUS: the broker may have stored
    /// the message. <c>PUBLISH_CONFIRM_TIMEOUT_MS</c>, default 30000 ms.
    /// </summary>
    public static long PublishConfirmTimeoutMs => EnvInt("PUBLISH_CONFIRM_TIMEOUT_MS", 30000);

    /// <summary>
    /// Heartbeat, in seconds. A <c>heartbeat</c> in the broker URL wins, and <c>heartbeat=0</c>
    /// there disables it. <c>AMQP_HEARTBEAT_SECONDS</c>, default 30.
    /// </summary>
    public static long HeartbeatSeconds => EnvInt("AMQP_HEARTBEAT_SECONDS", 30);

    /// <summary>How long a publish waits through a reconnection. <c>CONNECTION_READY_TIMEOUT_MS</c>, default 30000 ms.</summary>
    public static long ConnectionReadyTimeoutMs => EnvInt("CONNECTION_READY_TIMEOUT_MS", 30000);

    /// <summary>
    /// Publishes the broker has not yet answered, per channel, a timed-out one included; further
    /// ones wait for a slot. <c>MAX_OUTSTANDING_CONFIRMS</c>, default 256.
    /// </summary>
    public static long MaxOutstandingConfirms => EnvInt("MAX_OUTSTANDING_CONFIRMS", 256);

    /// <summary>Unconsumed chunks per stream. <c>STREAM_MAX_BUFFERED_CHUNKS</c>, default 1024.</summary>
    public static long StreamMaxBufferedChunks => EnvInt("STREAM_MAX_BUFFERED_CHUNKS", 1024);

    /// <summary>Unconsumed bytes per stream. <c>STREAM_MAX_BUFFERED_BYTES</c>, default 64 MiB.</summary>
    public static long StreamMaxBufferedBytes => EnvInt("STREAM_MAX_BUFFERED_BYTES", 64L * 1024 * 1024);

    /// <summary>Unconsumed bytes across a context's streams. <c>STREAM_MAX_TOTAL_BUFFERED_BYTES</c>, default 256 MiB.</summary>
    public static long StreamMaxTotalBufferedBytes => EnvInt("STREAM_MAX_TOTAL_BUFFERED_BYTES", 256L * 1024 * 1024);

    /// <summary>How long shutdown waits for in-flight work. <c>SHUTDOWN_DRAIN_TIMEOUT_MS</c>, default 30000 ms.</summary>
    public static long ShutdownDrainTimeoutMs => EnvInt("SHUTDOWN_DRAIN_TIMEOUT_MS", 30000);
}
