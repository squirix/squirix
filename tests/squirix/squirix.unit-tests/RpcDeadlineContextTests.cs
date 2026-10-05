using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
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
        using var outer = RpcDeadlineContext.Push(deadline, TimeProvider.System);
        using var inner = RpcDeadlineContext.Push(deadline, TimeProvider.System);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(deadline);
    }

    /// <summary>When the outer scope ended first, disposing the inner scope restores the deadline from before the outer one.</summary>
    [Test]
    public async Task InnerDisposeSkipsEndedOuterScope()
    {
        var outerDeadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        var outer = RpcDeadlineContext.Push(outerDeadline, TimeProvider.System);
        var inner = RpcDeadlineContext.Push(outerDeadline.AddSeconds(-10), TimeProvider.System);
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
        using var outer = RpcDeadlineContext.Push(outerDeadline, TimeProvider.System);
        using var inner = RpcDeadlineContext.Push(innerDeadline, TimeProvider.System);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(innerDeadline);
    }

    /// <summary>A local deadline is stored as UTC without changing the instant.</summary>
    [Test]
    public async Task PushConvertsLocalDeadlineToUtc()
    {
        var local = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Local);
        using (RpcDeadlineContext.Push(local, new FrozenClock(local.ToUniversalTime() - TimeSpan.FromSeconds(5))))
        {
            var remaining = RpcDeadlineContext.GetRemainingBudget();
            _ = await Assert.That(remaining).IsEqualTo(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>Pushing a deadline exposes the remaining budget until the scope is disposed of.</summary>
    [Test]
    public async Task PushExposesBudgetAndRestoresPrevious()
    {
        _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsNull();

        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        using (RpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime.AddSeconds(30), clock))
        {
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(30));
            clock.Advance(TimeSpan.FromSeconds(12));
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(18));
        }

        _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsNull();
    }

    /// <summary>Sentinel deadlines normalize to no ambient budget.</summary>
    [Test]
    public async Task PushNormalizesSpecialDeadlines()
    {
        using (RpcDeadlineContext.Push(null, TimeProvider.System))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsNull();

        using (RpcDeadlineContext.Push(DateTime.MaxValue, TimeProvider.System))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsNull();

        using (RpcDeadlineContext.Push(DateTime.MinValue, TimeProvider.System))
            _ = await Assert.That(RpcDeadlineContext.GetRemainingBudget()).IsNull();
    }

    /// <summary>Disposing a scope a second time after a later push leaves the newer deadline in place.</summary>
    [Test]
    public async Task RepeatedDisposeKeepsNewerDeadline()
    {
        var firstDeadline = new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);
        var secondDeadline = firstDeadline.AddSeconds(30);
        var first = RpcDeadlineContext.Push(firstDeadline, TimeProvider.System);
        first.Dispose();
        using var second = RpcDeadlineContext.Push(secondDeadline, TimeProvider.System);

        first.Dispose();

        _ = await Assert.That(RpcDeadlineContext.CurrentDeadlineUtc).IsEqualTo(secondDeadline);
    }

    /// <summary>A clock frozen at one instant, so a pushed budget reads back exactly.</summary>
    [Immutable]
    private sealed class FrozenClock : TimeProvider
    {
        private readonly DateTimeOffset _utcNow;

        internal FrozenClock(DateTime utcNow)
        {
            _utcNow = new DateTimeOffset(utcNow, TimeSpan.Zero);
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => 0;
    }
}
