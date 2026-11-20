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

    /// <summary>Child execution contexts inherit outer scopes and dispose their own scopes independently.</summary>
    [Test]
    public async Task NestedScopesStayInTheirAsyncFlow()
    {
        var scopes = new LoggerExternalScopeProvider();
        using var outer = scopes.Push("outer");
        var child = CollectChildAsync(scopes);
        var parent = Collect(scopes);
        var nested = await child;

        _ = await Assert.That(string.Join(',', parent)).IsEqualTo("outer");
        _ = await Assert.That(string.Join(',', nested)).IsEqualTo("outer,child");
        _ = await Assert.That(string.Join(',', Collect(scopes))).IsEqualTo("outer");
    }

    private static async Task<List<string>> CollectChildAsync(LoggerExternalScopeProvider scopes)
    {
        using var inner = scopes.Push("child");
        await Task.Yield();
        return Collect(scopes);
    }

    private static List<string> Collect(LoggerExternalScopeProvider scopes)
    {
        var active = new List<string>();
        scopes.ForEachScope(static (scope, target) => target.Add(scope as string ?? "<unexpected scope>"), active);
        return active;
    }

    private sealed class ErrorSink : ILogger
    {
        internal List<(int Id, Exception? Cause)> Events { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Events.Add((eventId.Id, exception));
    }
}
