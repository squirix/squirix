using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>RF=2 commits whose post-majority memory apply fails and is completed by a later commit.</summary>
[Immutable]
public sealed class ReplicaCommitStallTests : ServerUnitTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// An entry whose first apply failed after its majority is re-applied by the next commit; a retry of its operation then replays the
    /// committed outcome instead of reporting an unknown outcome, and the entry is not applied again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReappliedPendingEntryResolvesIdempotency(CancellationToken cancellationToken)
    {
        var pipeline = new ReplicaCommitTestKit.FailFirstApplyPipeline();
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 0, 0, 1),
            pipeline,
            ReplicaFaultHooks.CreateCancellationHonoring(),
            new GroupIdempotencyState(4, TimeSpan.MaxValue));
        var first = CreateMutation(1, "00000000000000000000000000000001", 11);
        var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(coordinator.CommitAsync(first, StallTimeout, cancellationToken));
        _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

        _ = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002", 12), StallTimeout, cancellationToken);
        var retried = await coordinator.CommitAsync(first, StallTimeout, cancellationToken);

        await SequenceAssert.EqualMemoryAsync(first.OutcomePayload, retried);
        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.AppliedIndexes);
    }

    private static PreparedReplicaMutation CreateMutation(ulong logIndex, string operationId, byte outcome) => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { outcome }, 4));
}
