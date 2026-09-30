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
    /// <summary>Disposing an outer scope while an inner scope with the same deadline is current leaves the inner scope in place.</summary>
    [Test]
    public async Task EqualDeadlineDisposeKeepsInnerScope()
    {
        var deadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        using var outer = RpcDeadlineContext.Push(deadline);
        using var inner = RpcDeadlineContext.Push(deadline);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(deadline);
    }

    /// <summary>When the outer scope ended first, disposing the inner scope restores the deadline from before the outer one.</summary>
    [Test]
    public async Task InnerDisposeSkipsEndedOuterScope()
    {
        var outerDeadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        var outer = RpcDeadlineContext.Push(outerDeadline);
        var inner = RpcDeadlineContext.Push(outerDeadline.AddSeconds(-10));
        outer.Dispose();

        inner.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsNull();
    }

    /// <summary>Disposing an outer scope while an inner scope is current leaves the inner deadline in place.</summary>
    [Test]
    public async Task OutOfOrderDisposeKeepsInnerDeadline()
    {
        var outerDeadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        var innerDeadline = outerDeadline.AddSeconds(-10);
        using var outer = RpcDeadlineContext.Push(outerDeadline);
        using var inner = RpcDeadlineContext.Push(innerDeadline);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(innerDeadline);
    }

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

    /// <summary>Disposing a scope a second time after a later push leaves the newer deadline in place.</summary>
    [Test]
    public async Task RepeatedDisposeKeepsNewerDeadline()
    {
        var firstDeadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        var secondDeadline = firstDeadline.AddSeconds(30);
        var first = RpcDeadlineContext.Push(firstDeadline);
        first.Dispose();
        using var second = RpcDeadlineContext.Push(secondDeadline);

        first.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(secondDeadline);
    }
}
