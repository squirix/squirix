using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for the ambient absolute-deadline context shared with transport retries.</summary>
[Immutable]
public sealed class RpcDeadlineContextTests : UnitTestBase
{
    /// <summary>A local deadline is stored as UTC without changing the instant.</summary>
    [Test]
    public async Task PushConvertsLocalDeadlineToUtc()
    {
        var local = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Local);
        using (RpcDeadlineContext.Push(local))
        {
            var remaining = RpcDeadlineContext.GetRemainingBudget(local.ToUniversalTime() - TimeSpan.FromSeconds(5));
            _ = await Assert.That(remaining).IsEqualTo(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>Pushing a deadline exposes the remaining budget until the scope is disposed of.</summary>
    [Test]
    public async Task PushExposesBudgetAndRestoresPrevious()
    {
        _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow)).IsNull();

        var deadline = DateTime.UtcNow.AddSeconds(30);
        using (RpcDeadlineContext.Push(deadline))
        {
            var remaining = RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            _ = await Assert.That(remaining is { } budget && budget > TimeSpan.Zero && budget <= TimeSpan.FromSeconds(30)).IsTrue();
        }

        _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow)).IsNull();
    }

    /// <summary>Sentinel deadlines normalize to no ambient budget.</summary>
    [Test]
    public async Task PushNormalizesSpecialDeadlines()
    {
        using (RpcDeadlineContext.Push(null))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow)).IsNull();

        using (RpcDeadlineContext.Push(DateTime.MaxValue))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow)).IsNull();

        using (RpcDeadlineContext.Push(DateTime.MinValue))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow)).IsNull();
    }
}
