using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A run torn by a crash recovers as a prefix that a later current-term commit applies.</summary>
[Immutable]
public sealed class ReplicaCommitBatchRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A run of four entries appended as one request and torn inside the third frame recovers the first two as an uncommitted tail; a
    /// coordinator started on that tail commits and applies them with the first entry of the new term, in log order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornRunPrefixIsCommittedByNextTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-recovery-torn-run");
        using var whole = new TempDirectory("squirix-batch-recovery-torn-run-whole");
        using var prefix = new TempDirectory("squirix-batch-recovery-torn-run-prefix");
        var wholeLength = await AppendRunAsync(whole, 3, cancellationToken);
        var prefixLength = await AppendRunAsync(prefix, 2, cancellationToken);
        _ = await AppendRunAsync(dir, 4, cancellationToken);
        var logPath = GroupLogFile.ActivePath(dir, GroupId);
        var written = await File.ReadAllBytesAsync(logPath, cancellationToken);
        await File.WriteAllBytesAsync(logPath, written.AsMemory(0, int.CreateChecked(prefixLength + ((wholeLength - prefixLength) / 2))).ToArray(), cancellationToken);
        await using var recovered = OpenLog(dir);
        await recovered.OpenAsync(cancellationToken);
        var tail = new ReplicaRecoveredTail(await recovered.GetUncommittedTailAsync(cancellationToken), 2, new TailRebuilder());
        _ = await Assert.That(tail.LastIndex).IsEqualTo(2UL);

        var pipeline = new RunPipeline();
        var idempotency = new GroupIdempotencyState(16, TimeSpan.MaxValue, TimeProvider.System);
        await using var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, tail.LastIndex, tail.FirstIndex - 1, 2),
            pipeline,
            ReplicaFaultHooks.CreateNoOp(),
            idempotency,
            null,
            tail)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
        };
        coordinator.AdmitReplica(1, tail.LastIndex);
        _ = await Assert.That(coordinator.HasPendingApply).IsTrue();

        _ = await coordinator.CommitAsync(Mutation(3, 2), StallTimeout, cancellationToken);

        await SequenceAssert.EqualAsync([1UL, 2UL, 3UL], pipeline.Applied.ToArray());
        await SequenceAssert.EqualAsync([3UL], pipeline.CommitIndexes.ToArray());
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
    }

    private static async Task<long> AppendRunAsync(TempDirectory dir, int count, CancellationToken cancellationToken)
    {
        string[] payloads = ["a", "b", "c", "d"];
        var entries = new FollowerLogEntry[count];
        for (var i = 0; i < count; i++)
            entries[i] = new FollowerLogEntry(ulong.CreateChecked(i + 1), 1UL, Encoding.UTF8.GetBytes(payloads[i]));

        await using (var log = OpenLog(dir))
        {
            await log.OpenSeededAsync(cancellationToken);
            var result = await log.AppendAsync(new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, entries), cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();
        }

        return FollowerLogTestKit.GetLogLength(GroupLogFile.ActivePath(dir, GroupId));
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);

    private static PreparedReplicaMutation Mutation(ulong logIndex, ulong term) => new(
        new ReplicaOperationIdentity("group-a", "client", logIndex.ToString("x32", CultureInfo.InvariantCulture), new byte[] { 1 }),
        term,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { byte.CreateChecked(10 + logIndex) }, 4));

    /// <summary>Rebuilds the prepared form of a recovered entry from its index and term alone.</summary>
    [Immutable]
    private sealed class TailRebuilder : IReplicaTailRebuilder
    {
        public PreparedReplicaMutation Rebuild(FollowerLogEntry entry) => Mutation(entry.LogIndex, entry.Term);
    }
}
