using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Logging;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Checks event formatting, severity filters and scope lifetimes.</summary>
public sealed class StructuredLogTests
{
    /// <summary>Formatting applies to the text while the sink receives the original field values.</summary>
    [Test]
    public async Task StructuredFieldsRetainRawValues()
    {
        const string template = "count={Count:D3} missing={Missing}";
        var descriptor = new LogEvent(1, "example", template);
        var entry = new LogEventState<(int, string?)>(descriptor, (7, null));

        _ = await Assert.That(entry.ToString()).IsEqualTo("count=007 missing=(null)");
        _ = await Assert.That(entry.Count).IsEqualTo(3);
        _ = await Assert.That(entry[0]).IsEqualTo(new KeyValuePair<string, object?>("Count", 7));
        _ = await Assert.That(entry[1]).IsEqualTo(new KeyValuePair<string, object?>("Missing", null));
        _ = await Assert.That(entry[2]).IsEqualTo(new KeyValuePair<string, object?>("{OriginalFormat}", template));
    }

    /// <summary>A sink that accepts only errors sees the matching event once.</summary>
    [Test]
    public async Task WritersHonorSeverityBeforeDispatch()
    {
        var sink = new ErrorSink();
        var failure = new InvalidOperationException("commit interrupted");
        RecoveryLog.RecoveryComplete(sink, 1, 2);
        RecoveryLog.JournalRecoveryFailed(sink, failure);

        _ = await Assert.That(sink.Events.Count).IsEqualTo(1);
        _ = await Assert.That(sink.Events[0].Id).IsEqualTo(2004);
        _ = await Assert.That(sink.Events[0].Cause).IsSameReferenceAs(failure);
    }

    private sealed class ErrorSink : IServerLogger
    {
        internal List<(int Id, Exception? Cause)> Events { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(ServerLogLevel logLevel) => logLevel >= ServerLogLevel.Error;

        public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Events.Add((eventId.Id, exception));
    }
}
