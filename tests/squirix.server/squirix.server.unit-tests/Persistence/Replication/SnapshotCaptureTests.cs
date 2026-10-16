using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Capturing the group snapshot at the durable applied index: each outcome, and that a capture writes nothing.</summary>
[Immutable]
public sealed class SnapshotCaptureTests : ServerUnitTestBase
{
    private const string GroupId = StateInstallScenario.GroupId;

    /// <summary>The captured snapshot equals the one a compaction publishes at the same index, and the capture writes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureEqualsCompactionSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-capture-equals");
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = clock };
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options);
        await log.OpenSeededAsync(cancellationToken);
        await log.AdoptTopologyAsync(new byte[] { 9, 8, 7 }, 2UL, cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 2UL, 1UL, "b"), cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
        _ = log.Idempotency.Reserve("client", "operation-1", [1, 2, 3], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = log.Idempotency.TryResolve("client", "operation-1", [9], 1UL, 1UL);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);
        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var metaBefore = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        var logBefore = await StateInstallScenario.ReadLogBytesAsync(dir, cancellationToken);

        var (groupSnapshotCaptureOutcome, captured) = await log.CaptureSnapshotAsync(cancellationToken);

        var metaAfter = await GroupMetadataFile.ReadBytesAsync(metaPath, cancellationToken);
        var logAfter = await StateInstallScenario.ReadLogBytesAsync(dir, cancellationToken);
        await SequenceAssert.EqualAsync(metaBefore, metaAfter);
        await SequenceAssert.EqualAsync(logBefore, logAfter);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsFalse();
        _ = await Assert.That(groupSnapshotCaptureOutcome).IsEqualTo(GroupSnapshotCaptureOutcome.Captured);

        _ = await Assert.That(await log.CompactThroughAsync(3UL, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        var published = await Assert.That(await new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).ReadPublishedAsync(cancellationToken)).IsNotNull();
        _ = await Assert.That((captured.GroupId, captured.ConfigurationGeneration, captured.LastIncludedTerm, captured.LastIncludedIndex, captured.CommitIndex, captured.CapturedUtc))
                        .IsEqualTo((published.GroupId, published.ConfigurationGeneration, published.LastIncludedTerm, published.LastIncludedIndex, published.CommitIndex, published.CapturedUtc));
        await SequenceAssert.EqualAsync(published.TopologyFingerprint.ToArray(), captured.TopologyFingerprint.ToArray());
        var outcome = await Assert.That(captured.CommittedOutcomes).HasSingleItem();
        var publishedOutcome = await Assert.That(published.CommittedOutcomes).HasSingleItem();
        _ = await Assert.That((outcome.OperationId, outcome.LogIndex, outcome.ResolvedUtc)).IsEqualTo((publishedOutcome.OperationId, publishedOutcome.LogIndex, publishedOutcome.ResolvedUtc));
        await SequenceAssert.EqualAsync(publishedOutcome.OutcomePayload.ToArray(), outcome.OutcomePayload.ToArray());
    }

    /// <summary>The captured snapshot is taken at the applied index, not at the commit index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureIsTakenAtAppliedIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-capture-applied");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);

        var capture = await log.CaptureSnapshotAsync(cancellationToken);

        _ = await Assert.That((capture.Outcome, capture.Snapshot.LastIncludedIndex, capture.Snapshot.CommitIndex)).IsEqualTo((GroupSnapshotCaptureOutcome.Captured, 2UL, 2UL));
    }

    /// <summary>A log that is not open reports not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureRefusesLogThatIsNotOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-capture-not-open");
        await using var log = StateInstallScenario.NewLog(dir);

        var capture = await log.CaptureSnapshotAsync(cancellationToken);

        _ = await Assert.That(capture.Outcome).IsEqualTo(GroupSnapshotCaptureOutcome.NotReady);
    }

    /// <summary>An unseeded copy and an incomplete copy are not complete, so neither is captured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureRefusesCopyThatIsNotComplete(CancellationToken cancellationToken)
    {
        using var unseededDir = new TempDirectory("squirix-capture-unseeded");
        using var incompleteDir = new TempDirectory("squirix-capture-incomplete");
        await using var unseeded = StateInstallScenario.NewLog(unseededDir);
        await unseeded.OpenAsync(cancellationToken);
        await using var incomplete = StateInstallScenario.NewLog(incompleteDir);
        await StateInstallScenario.OpenBehindAsync(incomplete, cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(incomplete, 2UL, 1UL, cancellationToken)).IsNull();

        var fromUnseeded = await unseeded.CaptureSnapshotAsync(cancellationToken);
        var fromIncomplete = await incomplete.CaptureSnapshotAsync(cancellationToken);

        _ = await Assert.That((fromUnseeded.Outcome, fromIncomplete.Outcome)).IsEqualTo((GroupSnapshotCaptureOutcome.NotComplete, GroupSnapshotCaptureOutcome.NotComplete));
    }

    /// <summary>A complete copy that applied nothing has no state to transfer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureRefusesNothingApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-capture-nothing");
        await using var log = StateInstallScenario.NewLog(dir);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

        var capture = await log.CaptureSnapshotAsync(cancellationToken);

        _ = await Assert.That(capture.Outcome).IsEqualTo(GroupSnapshotCaptureOutcome.NothingApplied);
    }

    /// <summary>An entry at or below the applied index with an unresolved outcome keeps the snapshot from being captured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureRefusesUnresolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-capture-unresolved");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await Assert.That(log.Idempotency.Reserve("client", "pending", [1], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);

        var capture = await log.CaptureSnapshotAsync(cancellationToken);

        _ = await Assert.That(capture.Outcome).IsEqualTo(GroupSnapshotCaptureOutcome.UnresolvedOutcome);
    }
}
