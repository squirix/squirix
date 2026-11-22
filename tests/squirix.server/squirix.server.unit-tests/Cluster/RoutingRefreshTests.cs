using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Stale-term server responses reroute at most once to another endpoint.</summary>
[Immutable]
public sealed class RoutingRefreshTests : ServerUnitTestBase
{
    /// <summary>A stale term consumes the single reroute; a second stale term stops instead of bouncing.</summary>
    [Test]
    public async Task StaleTermCausesAtMostOneServerReroute()
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        var budget = new RerouteBudget(deadline, TimeProvider.System);

        var first = new ServerCallException(FrameStatus.FailedPrecondition, RefusalCodes.StaleTerm);
        _ = await Assert.That(StaleTermClassifier.Classify(first)).IsEqualTo(StaleTermVerdict.Stale);
        _ = await Assert.That(budget.TryConsumeReroute()).IsTrue();

        var second = new ServerCallException(FrameStatus.FailedPrecondition, RefusalCodes.StaleTerm);
        _ = await Assert.That(StaleTermClassifier.Classify(second)).IsEqualTo(StaleTermVerdict.Stale);
        _ = await Assert.That(budget.TryConsumeReroute()).IsFalse();

        _ = await Assert.That(StaleTermClassifier.Classify(FrameStatus.FailedPrecondition, "other-detail")).IsEqualTo(StaleTermVerdict.Current);
        _ = await Assert.That(StaleTermClassifier.Classify(FrameStatus.Unavailable, RefusalCodes.StaleTerm)).IsEqualTo(StaleTermVerdict.Current);
        _ = await Assert.That(StaleTermClassifier.Classify(FrameStatus.Ok, RefusalCodes.StaleTerm)).IsEqualTo(StaleTermVerdict.Current);
    }

    /// <summary>A budget built from the remaining time of an operation expires with it; one without a deadline never expires.</summary>
    [Test]
    public async Task RerouteBudgetFromRemainingBudget()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var bounded = RerouteBudget.FromRemaining(TimeSpan.FromSeconds(1), clock);
        var unbounded = RerouteBudget.FromRemaining(null, clock);
        var spent = RerouteBudget.FromRemaining(TimeSpan.FromSeconds(-1), clock);

        _ = await Assert.That(bounded.GetRemaining()).IsEqualTo(TimeSpan.FromSeconds(1));
        _ = await Assert.That(spent.HasExpired()).IsTrue();
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(bounded.HasExpired()).IsTrue();
        _ = await Assert.That(unbounded.HasExpired()).IsFalse();
        _ = await Assert.That(RerouteBudget.FromRemaining(TimeSpan.MaxValue, clock).HasExpired()).IsFalse();
    }
}
