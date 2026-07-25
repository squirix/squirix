using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers structured correlation scope state enumeration.</summary>
public sealed class CorrelationScopeTests : ServerUnitTestBase
{
    /// <summary>Scope without a method exposes trace, span, and node fields.</summary>
    [Fact]
    public void BeginStandardScopeWithoutMethodExposesThreeFields()
    {
        using var activity = new Activity("corr-test");
        _ = activity.Start();
        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-a");
        var state = Assert.IsType<IReadOnlyList<KeyValuePair<string, object?>>>(logger.LastState, false);
        Assert.Equal(3, state.Count);
        Assert.Equal("trace_id", state[0].Key);
        Assert.Equal(activity.TraceId.ToString(), state[0].Value);
        Assert.Equal("span_id", state[1].Key);
        Assert.Equal(activity.SpanId.ToString(), state[1].Value);
        Assert.Equal("node_id", state[2].Key);
        Assert.Equal("node-a", state[2].Value);

        using var enumerator = state.GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());

        IEnumerable enumerable = state;
        using var nonGeneric = enumerable.GetEnumerator() as IDisposable;
        Assert.NotNull(nonGeneric);
    }

    /// <summary>Scope with a method includes the rpc.method field.</summary>
    [Fact]
    public void BeginStandardScopeWithMethodExposesFourFields()
    {
        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-b", "GetEntry");
        var state = Assert.IsType<IReadOnlyList<KeyValuePair<string, object?>>>(logger.LastState, false);
        Assert.Equal(4, state.Count);
        Assert.Equal(string.Empty, state[0].Value);
        Assert.Equal(string.Empty, state[1].Value);
        Assert.Equal("node-b", state[2].Value);
        Assert.Equal("rpc.method", state[3].Key);
        Assert.Equal("GetEntry", state[3].Value);

        using var enumerator = state.GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
    }

    /// <summary>Indexer rejects out-of-range access.</summary>
    [Fact]
    public void ScopeStateIndexerRejectsOutOfRange()
    {
        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-c");
        var state = Assert.IsType<IReadOnlyList<KeyValuePair<string, object?>>>(logger.LastState, false);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(state, static list => _ = list[99]);
    }

    private sealed class CapturingLogger : ILogger
    {
        internal object? LastState { get; private set; }

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
        {
            LastState = state;
            return Noop.Instance;
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }

        private sealed class Noop : IDisposable
        {
            internal static readonly Noop Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
