using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Restore behaviour of the server-side ambient deadline scope.</summary>
[Immutable]
public sealed class ServerRpcDeadlineContextTests : ServerUnitTestBase
{
    private static readonly TimeSpan CallerBudget = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan CappedBudget = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Elapsed = TimeSpan.FromSeconds(4);
    private static readonly TimeSpan LongerCap = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ShorterCap = TimeSpan.FromSeconds(3);
    private static readonly DateTime Now = new(2026, 3, 10, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A clock frozen at <see cref="Now" />, so a pushed budget reads back exactly.</summary>
    private static readonly FakeTimeProvider Clock = new(new DateTimeOffset(Now));

    /// <summary>Disposing an outer scope while an inner scope with the same deadline is current leaves the inner scope in place.</summary>
    [Test]
    public async Task EqualDeadlineDisposeKeepsInnerScope()
    {
        using var outer = ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock);
        using var inner = ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(10));
    }

    /// <summary>When the outer scope ended first, disposing the inner scope restores the deadline from before the outer one.</summary>
    [Test]
    public async Task InnerDisposeSkipsEndedOuterScope()
    {
        var outer = ServerRpcDeadlineContext.Push(Now.AddSeconds(30), Clock);
        var inner = ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock);
        outer.Dispose();

        inner.Dispose();

        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsNull();
    }

    /// <summary>Disposing an outer scope while an inner scope is current leaves the inner deadline in place.</summary>
    [Test]
    public async Task OutOfOrderDisposeKeepsInnerDeadline()
    {
        using var outer = ServerRpcDeadlineContext.Push(Now.AddSeconds(30), Clock);
        using var inner = ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock);

        // ReSharper disable once DisposeOnUsingVariable — intentional out-of-order dispose: the outer scope ends while the inner one is current.
        outer.Dispose();

        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(10));
    }

    /// <summary>Disposing a scope a second time after a later push leaves the newer deadline in place.</summary>
    [Test]
    public async Task RepeatedDisposeKeepsNewerDeadline()
    {
        var first = ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock);
        first.Dispose();
        using var second = ServerRpcDeadlineContext.Push(Now.AddSeconds(30), Clock);

        first.Dispose();

        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    /// <summary>A single scope restores the deadline that was current before it, including no deadline at all.</summary>
    [Test]
    public async Task ScopeRestoresPreviousDeadline()
    {
        using (ServerRpcDeadlineContext.Push(Now.AddSeconds(30), Clock))
        {
            using (ServerRpcDeadlineContext.Push(Now.AddSeconds(10), Clock))
                _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(10));

            _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(TimeSpan.FromSeconds(30));
        }

        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsNull();
    }

    /// <summary>A pushed budget becomes current without restarting, forwards its remaining time as the deadline, and restores the previous scope when disposed.</summary>
    [Test]
    public async Task PushedBudgetRestoresParent()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(Now));
        var budget = ServerDeadlineBudget.Start(Now + CallerBudget, clock).CappedAt(CappedBudget);
        using var outer = ServerRpcDeadlineContext.Push(Now + CallerBudget, clock);
        clock.Advance(Elapsed);
        var before = TimeProvider.System.GetUtcNow().UtcDateTime;

        TimeSpan? inside;
        DateTime? deadline;
        using (ServerRpcDeadlineContext.Push(in budget))
        {
            inside = ServerRpcDeadlineContext.GetRemainingBudget();
            deadline = ServerRpcDeadlineContext.EffectiveDeadline(null);
        }

        var after = TimeProvider.System.GetUtcNow().UtcDateTime;
        _ = await Assert.That(inside).IsEqualTo(CappedBudget - Elapsed);
        _ = await Assert.That(deadline >= before + inside && deadline <= after + inside).IsTrue();
        _ = await Assert.That(ServerRpcDeadlineContext.GetRemainingBudget()).IsEqualTo(CallerBudget - Elapsed);
    }

    /// <summary>Capping a budget keeps its start timestamp and never extends it.</summary>
    [Test]
    public async Task CappedBudgetKeepsStartAndNeverExtends()
    {
        var budget = ServerDeadlineBudget.Start(Now + CappedBudget, Clock);

        var shorter = budget.CappedAt(ShorterCap);
        var longer = budget.CappedAt(LongerCap);

        _ = await Assert.That(shorter.Budget).IsEqualTo(ShorterCap);
        _ = await Assert.That(shorter.StartedTimestamp).IsEqualTo(budget.StartedTimestamp);
        _ = await Assert.That(longer).IsEqualTo(budget);
    }
}
