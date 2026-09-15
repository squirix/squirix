using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers structured correlation scope state enumeration.</summary>
[Immutable]
public sealed class CorrelationScopeTests : ServerUnitTestBase
{
    /// <summary>Indexer rejects out-of-range access.</summary>
    [Test]
    public async Task ScopeStateIndexerRejectsOutOfRange()
    {
        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-c");
        var state = (await Assert.That(logger.LastState).IsAssignableTo<IReadOnlyList<KeyValuePair<string, object?>>>())!;
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(state, static list => _ = list[99]);
    }

    /// <summary>Scope with a method includes the rpc.method field.</summary>
    [Test]
    public async Task ScopeWithMethodExposesFourFields()
    {
        // TUnit runs each test inside its own tracing activity; clear the ambient
        // activity so this test observes the no-ambient scope fields like before.
        Activity.Current = null;

        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-b", "GetEntry");
        var state = (await Assert.That(logger.LastState).IsAssignableTo<IReadOnlyList<KeyValuePair<string, object?>>>())!;
        _ = await Assert.That(state.Count).IsEqualTo(4);
        _ = await Assert.That(state[0].Value).IsEqualTo(string.Empty);
        _ = await Assert.That(state[1].Value).IsEqualTo(string.Empty);
        _ = await Assert.That(state[2].Value).IsEqualTo("node-b");
        _ = await Assert.That(state[3].Key).IsEqualTo("rpc.method");
        _ = await Assert.That(state[3].Value).IsEqualTo("GetEntry");

        using var enumerator = state.GetEnumerator();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsFalse();
        enumerator.Reset();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
    }

    /// <summary>Scope without a method exposes trace, span, and node fields.</summary>
    [Test]
    public async Task ScopeWithoutMethodExposesThreeFields()
    {
        using var activity = new Activity("corr-test");
        _ = activity.Start();
        var logger = new CapturingLogger();
        using var scope = Correlation.BeginStandardScope(logger, "node-a");
        var state = (await Assert.That(logger.LastState).IsAssignableTo<IReadOnlyList<KeyValuePair<string, object?>>>())!;
        _ = await Assert.That(state.Count).IsEqualTo(3);
        _ = await Assert.That(state[0].Key).IsEqualTo("trace_id");
        _ = await Assert.That(state[0].Value).IsEqualTo(activity.TraceId.ToString());
        _ = await Assert.That(state[1].Key).IsEqualTo("span_id");
        _ = await Assert.That(state[1].Value).IsEqualTo(activity.SpanId.ToString());
        _ = await Assert.That(state[2].Key).IsEqualTo("node_id");
        _ = await Assert.That(state[2].Value).IsEqualTo("node-a");

        using var enumerator = state.GetEnumerator();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        _ = await Assert.That(enumerator.MoveNext()).IsFalse();

        IEnumerable enumerable = state;
        using var nonGeneric = enumerable.GetEnumerator() as IDisposable;
        _ = await Assert.That(nonGeneric).IsNotNull();
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

        [Immutable]
        private sealed class Noop : IDisposable
        {
            internal static readonly Noop Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
