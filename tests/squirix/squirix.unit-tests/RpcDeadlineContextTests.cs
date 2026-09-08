using System;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Unit tests for the ambient absolute-deadline context shared with transport retries.</summary>
[Immutable]
public sealed class RpcDeadlineContextTests : UnitTestBase
{
    /// <summary>Pushing a deadline exposes the remaining budget until the scope is disposed of.</summary>
    [Fact]
    public void PushExposesBudgetAndRestoresPrevious()
    {
        Assert.Null(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        var deadline = DateTime.UtcNow.AddSeconds(30);
        using (RpcDeadlineContext.Push(deadline))
        {
            var remaining = RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            Assert.True(remaining is { } budget && budget > TimeSpan.Zero && budget <= TimeSpan.FromSeconds(30));
        }

        Assert.Null(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));
    }

    /// <summary>Sentinel deadlines normalize to no ambient budget.</summary>
    [Fact]
    public void PushNormalizesSpecialDeadlines()
    {
        using (RpcDeadlineContext.Push(null))
            Assert.Null(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        using (RpcDeadlineContext.Push(DateTime.MaxValue))
            Assert.Null(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

        using (RpcDeadlineContext.Push(DateTime.MinValue))
            Assert.Null(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));
    }

    /// <summary>A local deadline is stored as UTC without changing the instant.</summary>
    [Fact]
    public void PushConvertsLocalDeadlineToUtc()
    {
        var local = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Local);
        using (RpcDeadlineContext.Push(local))
        {
            var remaining = RpcDeadlineContext.GetRemainingBudget(local.ToUniversalTime() - TimeSpan.FromSeconds(5));
            Assert.Equal(TimeSpan.FromSeconds(5), remaining);
        }
    }
}
