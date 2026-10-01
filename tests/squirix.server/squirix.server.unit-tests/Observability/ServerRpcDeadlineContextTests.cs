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
}
