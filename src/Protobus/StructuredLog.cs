using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Protobus;

/// <summary>What an operation came to.</summary>
public enum LogOutcome
{
    Ok,
    Confirmed,
    Failed,
    Timeout,
    Retried,
    Rejected,
    Dropped,
    Unroutable,
}

/// <summary>A sink that takes <see cref="Log"/>'s records as data rather than as formatted lines.</summary>
public interface IStructuredLogSink : ILogSink
{
    void Log(LogRecord record);
}

/// <summary>
/// One structured log line, as <see cref="Protobus.Log"/> builds it. Every text field is
/// sanitised: control characters become spaces and the value is capped at 256 characters (the
/// message at 1024). Absent fields are null.
/// </summary>
/// <param name="Component">always <c>protobus</c></param>
/// <param name="Level">debug, info, warn or error</param>
/// <param name="Timestamp">ISO-8601, UTC</param>
/// <param name="Diagnostics">
/// what <see cref="Protobus.Log.DiagnosticsSerializer"/> made of the call's diagnostics, or null when
/// none is installed
/// </param>
public sealed record LogRecord(
    string Component,
    string Level,
    string Timestamp,
    string Operation,
    string Message,
    string? MessageType = null,
    string? MessageId = null,
    string? CorrelationId = null,
    string? Service = null,
    string? Method = null,
    string? Queue = null,
    string? Exchange = null,
    string? RoutingKey = null,
    string? ErrorCode = null,
    string? ErrorName = null,
    string? Outcome = null,
    long? SizeBytes = null,
    long? DurationMs = null,
    long? Attempt = null,
    object? Diagnostics = null)
{
    /// <summary>The record as one line: <c>[protobus] operation: message (key=value ...)</c>.</summary>
    public string Format()
    {
        var detail = new StringBuilder();
        void Append(string key, object? value)
        {
            if (value == null) return;
            if (detail.Length > 0) detail.Append(' ');
            detail.Append(key).Append('=').Append(Convert.ToString(value, CultureInfo.InvariantCulture));
        }
        Append("messageType", MessageType);
        Append("messageId", MessageId);
        Append("correlationId", CorrelationId);
        Append("service", Service);
        Append("method", Method);
        Append("queue", Queue);
        Append("exchange", Exchange);
        Append("routingKey", RoutingKey);
        Append("errorCode", ErrorCode);
        Append("errorName", ErrorName);
        Append("outcome", Outcome);
        Append("sizeBytes", SizeBytes);
        Append("durationMs", DurationMs);
        Append("attempt", Attempt);
        Append("diagnostics", Diagnostics);
        return $"[{Component}] {Operation}: {Message}" + (detail.Length > 0 ? $" ({detail})" : "");
    }
}

/// <summary>The fields of one structured line; set what applies.</summary>
public sealed class LogFields
{
    public LogFields(string? operation) => Operation = operation;

    public string? Operation { get; }
    public string? MessageType { get; init; }
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public string? Service { get; init; }
    public string? Method { get; init; }
    public string? Queue { get; init; }
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorName { get; init; }
    public LogOutcome? Outcome { get; init; }
    public long? SizeBytes { get; init; }
    public long? DurationMs { get; init; }
    public long? Attempt { get; init; }

    /// <summary>Raw diagnostics (payload, headers, error), built only when a serializer is installed.</summary>
    public Func<IReadOnlyDictionary<string, object?>>? Diagnostics { get; init; }
}

/// <summary>
/// Structured counterpart to <see cref="Logger"/>: an event described as data, with the sink
/// deciding its shape.
/// <code>
/// Log.Info("published request", new LogFields("publish")
/// {
///     MessageType = "example.Service.doThing",
///     CorrelationId = id,
///     SizeBytes = body.Length,
///     Outcome = LogOutcome.Confirmed,
/// });
/// </code>
/// An <see cref="IStructuredLogSink"/> receives the <see cref="LogRecord"/>; any other sink
/// receives <see cref="LogRecord.Format"/> at the matching severity. Level filtering happens first
/// either way. Diagnostics are opt-in and lazy: <see cref="LogFields.Diagnostics"/> runs only when
/// a <see cref="DiagnosticsSerializer"/> is installed, and whatever it returns is the only form
/// that reaches the record.
/// </summary>
public static class Log
{
    private const int FieldMax = 256;
    private const int MessageMax = 1024;
    private static readonly Regex Control = new("[\\x00-\\x1f\\x7f]+", RegexOptions.Compiled);
    private static volatile Func<IReadOnlyDictionary<string, object?>, LogRecord, object?>? serializer;

    /// <summary>Turns a call's raw diagnostics into what the record carries; return null to omit them.</summary>
    public static Func<IReadOnlyDictionary<string, object?>, LogRecord, object?>? DiagnosticsSerializer
    {
        get => serializer;
        set => serializer = value;
    }

    public static void Debug(string message, LogFields? fields) => Emit("debug", LogLevel.Debug, message, fields);
    public static void Info(string message, LogFields? fields) => Emit("info", LogLevel.Info, message, fields);
    public static void Warn(string message, LogFields? fields) => Emit("warn", LogLevel.Warn, message, fields);
    public static void Error(string message, LogFields? fields) => Emit("error", LogLevel.Error, message, fields);

    internal static string? Sanitize(string? value, int max)
    {
        if (value == null) return null;
        var text = Control.Replace(value, " ").Trim();
        if (text.Length == 0) return null;
        return text.Length > max ? text.Substring(0, max) : text;
    }

    private static string? Field(string? value) => Sanitize(value, FieldMax);

    internal static LogRecord Build(string level, string message, LogFields f) => new(
        "protobus", level, DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
        Field(f.Operation) ?? "unknown", Sanitize(message, MessageMax) ?? "",
        Field(f.MessageType), Field(f.MessageId), Field(f.CorrelationId), Field(f.Service), Field(f.Method),
        Field(f.Queue), Field(f.Exchange), Field(f.RoutingKey), Field(f.ErrorCode), Field(f.ErrorName),
        f.Outcome?.ToString().ToLowerInvariant(), f.SizeBytes, f.DurationMs, f.Attempt);

    private static void Emit(string levelName, LogLevel threshold, string message, LogFields? fields)
    {
        if (!Logger.Enabled(threshold)) return;
        var record = Build(levelName, message, fields ?? new LogFields(null));
        if (serializer is { } s && fields?.Diagnostics is { } diagnostics)
        {
            try
            {
                if (s(diagnostics(), record) is { } extra) record = record with { Diagnostics = extra };
            }
            catch (Exception)
            {
                // A failing hook must not take down the operation being logged, nor lose the line.
            }
        }
        var target = Logger.Sink;
        if (target is IStructuredLogSink structured)
        {
            try
            {
                structured.Log(record);
                return;
            }
            catch (Exception)
            {
                // A structured sink that throws degrades to the text path.
            }
        }
        var text = record.Format();
        switch (levelName)
        {
            case "debug": target.Debug(text); break;
            case "info": target.Info(text); break;
            case "warn": target.Warn(text); break;
            default: target.Error(text); break;
        }
    }
}
