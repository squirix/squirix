using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Publishes follower-log snapshots through the production compaction path, as the replica compaction step does.</summary>
internal static class FollowerSnapshotScenario
{
    /// <summary>Creates the one-shot fault that fails the first durable flush after it is armed.</summary>
    /// <returns>The fault hooks to pass to the log under test.</returns>
    internal static ArmableFlushFaultHooks CreateCompactionFaults() => new(static () => new IOException("simulated crash after the snapshot was published."));

    /// <summary>Marks the prefix applied, compacts it, and returns the snapshot the compaction published.</summary>
    /// <param name="log">The open log holding committed entries through <paramref name="index" />.</param>
    /// <param name="persistenceRoot">The node data directory the log was created under.</param>
    /// <param name="index">The committed index to compact through.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The published snapshot.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the compaction does not complete or publishes no snapshot.</exception>
    internal static async Task<GroupSnapshot> CompactThroughAsync(FollowerLog log, string persistenceRoot, ulong index, CancellationToken cancellationToken)
    {
        _ = await log.AdvanceAppliedAsync(index, cancellationToken);
        var outcome = await log.CompactThroughAsync(index, cancellationToken);
        var published = outcome == GroupCompactionOutcome.Compacted ? await new GroupSnapshotStore(persistenceRoot, log.GroupId).ReadPublishedAsync(cancellationToken) : null;
        return published ?? ThrowHelper.Throw<GroupSnapshot>(new InvalidOperationException($"Compaction through index {index} ended with '{outcome}' and no published snapshot."));
    }

    /// <summary>Marks the prefix applied and compacts it with the compaction failing after the snapshot is published, so the journal is left uncompacted.</summary>
    /// <param name="log">The open log, created with <paramref name="faults" />, holding committed entries through <paramref name="index" />.</param>
    /// <param name="faults">The fault hooks the log was created with.</param>
    /// <param name="index">The committed index to compact through.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the injected failure surfaced.</returns>
    /// <remarks>The log fails readiness afterwards and must be disposed and reopened, like after a crash.</remarks>
    internal static async Task PublishWithoutCompactionAsync(FollowerLog log, ArmableFlushFaultHooks faults, ulong index, CancellationToken cancellationToken)
    {
        _ = await log.AdvanceAppliedAsync(index, cancellationToken);
        faults.Arm();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(index, cancellationToken));
    }
}
