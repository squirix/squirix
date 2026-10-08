using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A follower out of the write quorum is queued for repair as soon as it answers the leader from its log.</summary>
[Immutable]
public sealed class ReplicaAnsweringFollowersTests : ServerUnitTestBase
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);
    private static readonly FollowerLogAppendResult Accepted = new(true, string.Empty, 2UL, 0UL);

    /// <summary>An accepted heartbeat or a log mismatch of a follower out of the quorum queues it at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AnswerOfNonReadyFollowerQueuesIt(CancellationToken cancellationToken)
    {
        var (answering, repairs, _, _) = Create();

        answering.Observe(1, in Accepted);
        var accepted = await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);
        answering.Observe(2, new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, 2UL, 0UL));
        var mismatched = await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

        _ = await Assert.That((accepted, mismatched)).IsEqualTo((true, true));
    }

    /// <summary>A ready or quarantined follower, and a refusal before the log or by a log not ready, queue nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherAnswersQueueNothing(CancellationToken cancellationToken)
    {
        var (answering, repairs, eligibility, _) = Create();
        var ready = new ReplicaProgress(4, 3, 3, 0, 1, ReadOnlyMemory<byte>.Of(9), 1, 0);
        _ = await Assert.That(eligibility.TryMarkReady(1, in ready, in ready)).IsTrue();
        eligibility.Quarantine(2);

        answering.Observe(1, in Accepted);
        answering.Observe(2, in Accepted);
        answering.Observe(0, new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, 2UL, 0UL));
        answering.Observe(0, new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, 3UL, 0UL));

        _ = await Assert.That(await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>A follower that keeps answering without becoming ready is queued again only once the interval elapsed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedAnswersQueueOncePerInterval(CancellationToken cancellationToken)
    {
        var (answering, repairs, _, clock) = Create();
        answering.Observe(1, in Accepted);
        _ = await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

        clock.Advance(Interval / 2);
        answering.Observe(1, in Accepted);
        var early = await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);
        clock.Advance(Interval / 2);
        answering.Observe(1, in Accepted);
        var later = await repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

        _ = await Assert.That((early, later)).IsEqualTo((false, true));
    }

    private static (ReplicaAnsweringFollowers Answering, ReplicaRepairQueue Repairs, ReplicaEligibility Eligibility, FakeTimeProvider Clock) Create()
    {
        var eligibility = new ReplicaEligibility(3);
        var repairs = new ReplicaRepairQueue(3);
        var clock = new FakeTimeProvider();
        return (new ReplicaAnsweringFollowers(eligibility, repairs, clock, Interval), repairs, eligibility, clock);
    }
}
