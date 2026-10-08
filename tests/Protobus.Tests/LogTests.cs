using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Xunit;

namespace Protobus.Tests;

public sealed class LogTests : IDisposable
{
    private sealed class TextSink : ILogSink
    {
        public readonly ConcurrentQueue<string> Lines = new();
        public void Debug(string message) => Lines.Enqueue("debug " + message);
        public void Info(string message) => Lines.Enqueue("info " + message);
        public void Warn(string message) => Lines.Enqueue("warn " + message);
        public void Error(string message) => Lines.Enqueue("error " + message);
    }

    private sealed class StructuredSink : IStructuredLogSink
    {
        public readonly ConcurrentQueue<LogRecord> Records = new();
        public readonly ConcurrentQueue<string> Lines = new();
        public bool Throw;
        public void Debug(string message) => Lines.Enqueue(message);
        public void Info(string message) => Lines.Enqueue(message);
        public void Warn(string message) => Lines.Enqueue(message);
        public void Error(string message) => Lines.Enqueue(message);

        public void Log(LogRecord record)
        {
            if (Throw) throw new InvalidOperationException("sink down");
            Records.Enqueue(record);
        }
    }

    public LogTests() => Logger.Level = LogLevel.Debug;

    public void Dispose()
    {
        Logger.Set(null);
        Logger.Level = LogLevel.Info;
        Protobus.Log.DiagnosticsSerializer = null;
    }

    [Fact]
    public void AStructuredSinkReceivesTheRecord()
    {
        var sink = new StructuredSink();
        Logger.Set(sink);
        Protobus.Log.Info("published request", new LogFields("publish")
        {
            MessageType = "example.Service.doThing",
            CorrelationId = "c-1",
            SizeBytes = 42,
            Outcome = LogOutcome.Confirmed,
        });
        var r = Assert.Single(sink.Records);
        Assert.Equal("protobus", r.Component);
        Assert.Equal("info", r.Level);
        Assert.Equal("publish", r.Operation);
        Assert.Equal("published request", r.Message);
        Assert.Equal("example.Service.doThing", r.MessageType);
        Assert.Equal("c-1", r.CorrelationId);
        Assert.Equal(42, r.SizeBytes);
        Assert.Equal("confirmed", r.Outcome);
        Assert.Null(r.Queue);
        Assert.Null(r.Diagnostics);
        Assert.True(DateTimeOffset.TryParse(r.Timestamp, out _));
        Assert.Empty(sink.Lines);
    }

    [Fact]
    public void AnyOtherSinkGetsOneFormattedLineAtTheRecordsLevel()
    {
        var sink = new TextSink();
        Logger.Set(sink);
        Protobus.Log.Warn("retrying", new LogFields("settle") { Queue = "Calc", Attempt = 2, Outcome = LogOutcome.Retried });
        Assert.Equal("warn [protobus] settle: retrying (queue=Calc outcome=retried attempt=2)", Assert.Single(sink.Lines));
    }

    [Fact]
    public void TheThresholdFiltersRecords()
    {
        var sink = new StructuredSink();
        Logger.Set(sink);
        Logger.Level = LogLevel.Warn;
        Protobus.Log.Info("quiet", new LogFields("x"));
        Protobus.Log.Error("loud", new LogFields("x"));
        Assert.Equal("error", Assert.Single(sink.Records).Level);
    }

    [Fact]
    public void FieldsAreSanitisedAndCapped()
    {
        var sink = new StructuredSink();
        Logger.Set(sink);
        Protobus.Log.Info("line\nbreak\u0007", new LogFields("op\r\n") { Queue = new string('q', 300), Exchange = " \t " });
        var r = Assert.Single(sink.Records);
        Assert.Equal("line break", r.Message);
        Assert.Equal("op", r.Operation);
        Assert.Equal(256, r.Queue!.Length);
        Assert.Null(r.Exchange);
        Protobus.Log.Info(new string('m', 2000), null);
        Assert.Equal(1024, sink.Records.ToArray()[1].Message.Length);
        Assert.Equal("unknown", sink.Records.ToArray()[1].Operation);
    }

    [Fact]
    public void DiagnosticsAreBuiltOnlyWhenASerializerIsInstalled()
    {
        var sink = new StructuredSink();
        Logger.Set(sink);
        var built = 0;
        Func<IReadOnlyDictionary<string, object?>> diagnostics = () =>
        {
            built++;
            return new Dictionary<string, object?> { ["payloadBytes"] = 3 };
        };
        Protobus.Log.Error("failed", new LogFields("handle") { Diagnostics = diagnostics });
        Assert.Equal(0, built);
        Assert.Null(sink.Records.ToArray()[0].Diagnostics);

        Protobus.Log.DiagnosticsSerializer = (raw, record) => $"{record.Operation}:{raw["payloadBytes"]}";
        Protobus.Log.Error("failed", new LogFields("handle") { Diagnostics = diagnostics });
        Assert.Equal(1, built);
        Assert.Equal("handle:3", sink.Records.ToArray()[1].Diagnostics);
    }

    [Fact]
    public void AFailingSerializerOrSinkNeverLosesTheLine()
    {
        var sink = new StructuredSink { Throw = true };
        Logger.Set(sink);
        Protobus.Log.DiagnosticsSerializer = (_, _) => throw new InvalidOperationException("boom");
        Protobus.Log.Error("still here", new LogFields("op") { Diagnostics = () => new Dictionary<string, object?>() });
        Assert.Equal("[protobus] op: still here", Assert.Single(sink.Lines));
    }
}
