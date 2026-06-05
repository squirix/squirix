using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Allocation-focused tests for observability scopes.
/// </summary>
public sealed class CorrelationAllocationTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies that <c>Correlation.BeginStandardScope</c> does not allocate a dictionary on the hot path.
    /// </summary>
    [Fact]
    public void BeginStandardScopeDoesNotAllocateDictionary()
    {
        var logger = new TestLogger();

        using var disposable = Correlation.BeginStandardScope(logger, "node-a", "Squirix.Service/Method");

        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            for (var i = 0; i < 10_000; i++)
            {
                using var scope = Correlation.BeginStandardScope(logger, "node-a", "Squirix.Service/Method");
            }
        });

        Assert.Equal(0, allocated);
    }

    /// <summary>
    /// Verifies that <c>Correlation.BeginStandardScope</c> forwards a structured scope state.
    /// </summary>
    [Fact]
    public void BeginStandardScopeUsesStructuredState()
    {
        var logger = new TestLogger();

        using var disposable = Correlation.BeginStandardScope(logger, "node-a", "Squirix.Service/Method");

        Assert.Same(NoopScope.Instance, disposable);
    }

    private sealed class NoopScope : IDisposable
    {
        public static readonly NoopScope Instance = new();

        public void Dispose()
        {
        }
    }

    private sealed class TestLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => NoopScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }
    }
}
