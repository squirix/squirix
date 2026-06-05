using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Behavioral tests for <see cref="Correlation.BeginStandardScope" /> scope nesting and logging integration.
/// </summary>
public sealed class CorrelationScopeBehaviorTests
{
    /// <summary>
    /// Verifies the structured scope contains trace, span, node, and method fields when an activity exists.
    /// </summary>
    [Fact]
    public void BeginStandardScopeCapturesStructuredFieldsFromActivity()
    {
        using var activity = new Activity("rpc");
        _ = activity.Start();
        var logger = new CapturingLogger();

        using (Correlation.BeginStandardScope(logger, "node-a", "Svc/Call"))
        {
        }

        Assert.NotNull(logger.LastScopeState);
        var state = logger.LastScopeState;
        Assert.Equal(4, state.Count);
        Assert.Equal(activity.TraceId.ToString(), state["trace_id"]);
        Assert.Equal(activity.SpanId.ToString(), state["span_id"]);
        Assert.Equal("node-a", state["node_id"]);
        Assert.Equal("Svc/Call", state["rpc.method"]);
    }

    /// <summary>
    /// Verifies the structured scope omits the rpc method field when it is not provided.
    /// </summary>
    [Fact]
    public void BeginStandardScopeOmitsMethodWhenNull()
    {
        var logger = new CapturingLogger();

        using (Correlation.BeginStandardScope(logger, "node-a"))
        {
        }

        Assert.NotNull(logger.LastScopeState);
        var state = logger.LastScopeState;
        Assert.Equal(3, state.Count);
        Assert.DoesNotContain("rpc.method", state.Keys);
    }

    /// <summary>
    /// Verifies <see cref="Correlation.BeginStandardScope" /> returns a usable disposable when the logger returns no scope.
    /// </summary>
    [Fact]
    public void BeginStandardScopeReturnsNoopWhenLoggerReturnsNullScope()
    {
        var logger = new NullScopeLogger();

        using var outer = Correlation.BeginStandardScope(logger, "node-a", "Rpc");

        Assert.NotNull(outer);
    }

    /// <summary>
    /// Verifies <see cref="Correlation.BeginStandardScope" /> works when <see cref="System.Diagnostics.Activity.Current" /> is unset.
    /// </summary>
    [Fact]
    public void BeginStandardScopeWorksWithoutActiveActivity()
    {
        var logger = new DepthTrackingLogger();

        using (Correlation.BeginStandardScope(logger, "node-z", "Squirix.Test/NoActivity"))
            Assert.Equal(1, logger.ScopeDepth);
    }

    /// <summary>
    /// Verifies nested scopes dispose in stack order so the outer scope remains active after the inner scope ends.
    /// </summary>
    [Fact]
    public void NestedStandardScopesRestoreOuterContext()
    {
        var logger = new DepthTrackingLogger();

        using (Correlation.BeginStandardScope(logger, "node-a", "Outer"))
        {
            Assert.Equal(1, logger.ScopeDepth);
            using (Correlation.BeginStandardScope(logger, "node-a", "Inner"))
                Assert.Equal(2, logger.ScopeDepth);

            Assert.Equal(1, logger.ScopeDepth);
        }

        Assert.Equal(0, logger.ScopeDepth);
    }

    private sealed class CapturingLogger : ILogger
    {
        public IReadOnlyDictionary<string, object?>? LastScopeState { get; private set; }

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
        {
            LastScopeState = ((IEnumerable<KeyValuePair<string, object?>>)state).ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
            return NullScope.Instance;
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }
    }

    private sealed class DepthTrackingLogger : ILogger
    {
        public int ScopeDepth { get; private set; }

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
        {
            ScopeDepth++;
            return new PopDisposable(() => ScopeDepth--);
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }

        private sealed class PopDisposable : IDisposable
        {
            private readonly Action _onDispose;
            private bool _disposed;

            public PopDisposable(Action onDispose)
            {
                _onDispose = onDispose;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;

                _disposed = true;
                _onDispose();
            }
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }

    private sealed class NullScopeLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => null!;

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }
    }
}
