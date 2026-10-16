using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Persistence.Replication.StateInstallLeaders;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Completing a state transfer on a follower log: what it publishes, replaces and restores, and what it refuses before writing anything.</summary>
public sealed class ReplicaSnapshotInstallTests : ServerUnitTestBase
{
    private const string GroupId = "grp-install";

    /// <summary>
    /// A transfer into a follower whose store already holds more committed outcomes than its capacity, of a snapshot that carries
    /// more than the capacity too, succeeds and keeps the log ready: committed outcomes are never refused for the capacity.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallPastCapacityStaysReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-capacity");
        var composition = GroupComposition.Create(GroupId);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, IdempotencyCapacity = 2 };

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, options);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        var now = DateTime.UtcNow;
        log.Idempotency.MarkOutcomesRebuilt();
        for (var i = 1; i <= 3; i++)
        {
            var index = ulong.CreateChecked(i);
            log.Idempotency.RecordCommittedOutcome(new GroupIdempotencyRecord("client", $"applied-{i}", new byte[] { 7 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, index, 1UL));
        }

        var outcomes = new List<GroupIdempotencyRecord>
        {
            new("client", "operation-1", new byte[] { 1 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL),
            new("client", "operation-2", new byte[] { 2 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL),
            new("client", "operation-3", new byte[] { 3 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 3UL, 1UL),
        };
        var oversized = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 3UL, 3UL, outcomes, now);

        var result = await FollowerSnapshotScenario.InstallAsync(log, oversized, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue().Because($"refused with '{result.RefusalCode}'");
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsTrue();
        for (var i = 0; i < outcomes.Count; i++)
            _ = await Assert.That(log.Idempotency.Lookup("client", outcomes[i].OperationId, outcomes[i].OperationFingerprint.Span, out _)).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A transfer at the applied index drops the covered frames: none is left to apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallAtAppliedDropsCoveredFrames(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-applied-watermark");
        using var sourceDir = new TempDirectory("squirix-install-applied-watermark-source");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await source.OpenSeededAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await source.AdvanceCommitAsync(3UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, 3UL, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(3UL, cancellationToken);

        var result = await FollowerSnapshotScenario.InstallAsync(log, snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That((await log.GetCommittedEntriesAsync(0UL, 10, cancellationToken)).Count).IsEqualTo(0);
    }

    /// <summary>An incomplete copy that holds nothing is moved forward to the snapshot boundary: an index above its applied index is what a transfer is for.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallMovesEmptyCopyToBoundary(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-boundary-source");
        using var dir2 = new TempDirectory("squirix-install-boundary-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await source.OpenSeededAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await source.AdvanceCommitAsync(3UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, dir, 3UL, cancellationToken);

        await using var target = new FollowerLog(dir2, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await target.OpenSeededAsync(cancellationToken);

        var result = await FollowerSnapshotScenario.InstallAsync(target, snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue().Because($"refused with '{result.RefusalCode}'");
        var status = await target.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 1UL, 3UL, 3UL));
        _ = await Assert.That(await target.GetTermAtAsync(3UL, cancellationToken)).IsEqualTo(1UL);
        _ = await Assert.That(new GroupSnapshotStore(dir2, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsTrue();
    }

    /// <summary>A transfer replaces the log by the snapshot baseline: the local entries, applied ones and a divergent tail alike, leave the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallReplacesLocalLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-replaces-log");
        using var sourceDir = new TempDirectory("squirix-install-replaces-log-source");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await source.OpenSeededAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AdvanceCommitAsync(2UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, 2UL, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "tail"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

        var result = await FollowerSnapshotScenario.InstallAsync(log, snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((2UL, 2UL, 2UL));
        _ = await Assert.That(await log.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
        _ = await Assert.That(FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId))).IsEqualTo(GroupLogCodec.LogFileHeader.Length);
    }

    /// <summary>Completing a transfer releases an unresolved reservation the snapshot did not export from the covered prefix, and keeps the resolved outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallReleasesPrefixReservation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-install");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);

        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.Reserve("client", "kept", [2], GroupRecordKind.UserMutation, 1UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.TryResolve("client", "kept", [3], 1UL, 1UL)).IsTrue();

        // Compaction refuses a prefix with an unresolved outcome, so only a transferred snapshot can drop one; this one exports just the resolved outcome.
        var now = DateTime.UtcNow;
        var kept = new GroupIdempotencyRecord("client", "kept", new byte[] { 2 }, new byte[] { 3 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, [kept], now);
        var install = await FollowerSnapshotScenario.InstallAsync(log, snapshot, 1UL, cancellationToken);
        _ = await Assert.That(install.Success).IsTrue();

        _ = await Assert.That(log.Idempotency.Lookup("client", "orphan", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(log.Idempotency.Lookup("client", "kept", [2], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.IsResolved).IsTrue();
        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 3UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>A transfer durably adopts a higher leader term, and the term survives a reopen.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallPersistsHigherLeaderTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-higher-term");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);
            var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

            var result = await FollowerSnapshotScenario.InstallAsync(log, snapshot, 5UL, cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(5UL);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(5UL);
    }

    /// <summary>Completing refuses a snapshot whose commit index falls below its included index, keeping watermarks coherent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesCommitBelowIncluded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-commit-below-included");
        await using var log = await OpenIncompleteAsync(dir, 2UL, cancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 1UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.CompleteStateInstallAsync(malformed, Leader(1UL), cancellationToken);

        await AssertRefusedUntouchedAsync(dir, log, result, cancellationToken);
    }

    /// <summary>Completing refuses a snapshot below the durable commit index: durable watermarks never move back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesBoundaryBelowCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-below-commit");
        await using var log = await OpenIncompleteAsync(dir, 2UL, cancellationToken);
        var below = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.CompleteStateInstallAsync(below, Leader(1UL), cancellationToken);

        await AssertRefusedUntouchedAsync(dir, log, result, cancellationToken);
    }

    /// <summary>Completing refuses a resolved outcome whose log index lies beyond the snapshot boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesOutcomeBeyondBoundary(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-outcome-beyond-boundary");
        await using var log = await OpenIncompleteAsync(dir, 2UL, cancellationToken);

        var now = DateTime.UtcNow;
        var outcome = new GroupIdempotencyRecord("client", "operation-1", new byte[] { 1, 2, 3 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 4UL, 1UL);
        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, new[] { outcome }, now);

        var result = await log.CompleteStateInstallAsync(malformed, Leader(1UL), cancellationToken);

        await AssertRefusedUntouchedAsync(dir, log, result, cancellationToken);
    }

    /// <summary>Completing refuses a snapshot whose committed outcomes contain an unresolved record, so it never publishes poisoned state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesUnresolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-unresolved-outcome");
        await using var log = await OpenIncompleteAsync(dir, 2UL, cancellationToken);

        var unresolved = new GroupIdempotencyRecord(
            "client",
            "operation-1",
            new byte[] { 1, 2, 3 },
            ReadOnlyMemory<byte>.Empty,
            GroupRecordKind.UserMutation,
            DateTime.UnixEpoch,
            null,
            1UL,
            1UL);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, new[] { unresolved }, DateTime.UnixEpoch);

        var result = await log.CompleteStateInstallAsync(malformed, Leader(1UL), cancellationToken);

        // The invalid snapshot must not have been published, so the next recovery is not poisoned.
        await AssertRefusedUntouchedAsync(dir, log, result, cancellationToken);
    }

    /// <summary>Completing refuses a snapshot whose LastIncludedTerm is zero, so the zero "unverifiable term" sentinel never becomes the baseline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesZeroIncludedTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-zero-term");
        await using var log = await OpenIncompleteAsync(dir, 2UL, cancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.CompleteStateInstallAsync(malformed, Leader(1UL), cancellationToken);

        await AssertRefusedUntouchedAsync(dir, log, result, cancellationToken);
    }

    /// <summary>Beginning and completing refuse a leader from a deposed term without touching durable state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesStaleLeaderTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-stale-term");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        var second = new FollowerLogAppendRequest(
            "leader",
            2UL,
            1UL,
            1UL,
            0UL,
            ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, BufferEx.CopyToOwned("b"u8))));
        _ = await log.AppendAsync(second, cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);
        var before = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

        var begin = await FollowerSnapshotScenario.BeginAsync(log, 2UL, 1UL, cancellationToken);

        _ = await Assert.That(begin).IsEqualTo(FollowerLogRefusal.StaleTerm);
        var after = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        await SequenceAssert.EqualAsync(before, after);

        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, 2UL, 2UL, cancellationToken)).IsNull();
        var complete = await log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancellationToken);

        _ = await Assert.That((complete.Success, complete.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.StaleTerm));
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.MemberState)).IsEqualTo((2UL, GroupMemberState.Incomplete));
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsFalse();
    }

    /// <summary>
    /// A durable fault during the log replacement must not leave the in-memory idempotency map holding entries the transfer discarded:
    /// idempotency is restored from the snapshot before the log is replaced, so a failed replacement still exposes the snapshot outcomes
    /// instead of stale prior-state lookups.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRestoresIdempotencyFirst(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-ordering");
        var composition = GroupComposition.Create(GroupId);

        var faults = new ArmableFlushFaultHooks(static () => new InvalidOperationException("injected install rewrite fault"));
        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, faults, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);

            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await Assert.That(log.Idempotency.Reserve("client", "op-A", [1], GroupRecordKind.UserMutation, 1UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
            _ = await Assert.That(log.Idempotency.TryResolve("client", "op-A", [9], 1UL, 1UL)).IsTrue();
            _ = await Assert.That(log.Idempotency.Lookup("client", "op-A", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

            var status = await log.GetStatusAsync(cancellationToken);
            const GroupRecordKind kind = GroupRecordKind.UserMutation;
            var outcome = new GroupIdempotencyRecord("client", "op-B", new byte[] { 2 }, new byte[] { 8 }, kind, DateTime.UtcNow, DateTime.UtcNow, 1UL, 1UL);
            var snapshot = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 1UL, 1UL, new List<GroupIdempotencyRecord> { outcome }, outcome.ResolvedUtc!.Value);

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidOperationException>(FollowerSnapshotScenario.InstallAsync(log, snapshot, 1UL, cancellationToken));

            _ = await Assert.That(log.Idempotency.Lookup("client", "op-A", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
            _ = await Assert.That(log.Idempotency.Lookup("client", "op-B", [2], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        // The durable outcome of the failed transfer matters after a crash: reopening the same directory must
        // recover consistent watermarks and the snapshot's idempotency state, never stale prior-state entries.
        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var rs = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(rs.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(rs.LastAppliedIndex <= rs.CommitIndex).IsTrue()
                        .Because($"Recovered applied watermark exceeds the commit watermark: {rs.LastAppliedIndex} > {rs.CommitIndex}.");
        _ = await Assert.That(rs.LastLogIndex >= rs.CommitIndex).IsTrue()
                        .Because($"Recovered log tail is shorter than the commit watermark: {rs.LastLogIndex} < {rs.CommitIndex}.");
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "op-A", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(reopened.Idempotency.Lookup("client", "op-B", [2], out var recovered)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(recovered.OutcomePayload.Span is [8]).IsTrue().Because("The recovered op-B outcome payload diverges from the snapshot outcome.");
    }

    /// <summary>A snapshot too large to publish fails before anything is written at the completion: the copy stays incomplete and the log stays ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedSnapshotLeavesCopyIncomplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-oversized");
        await using var log = OpenLimited(dir, 400);
        await log.OpenSeededAsync(cancellationToken);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, 1UL, 1UL, cancellationToken)).IsNull();

        _ = await NodeAsyncAssert.ThrowsAsync<GroupSnapshotTooLargeException>(log.CompleteStateInstallAsync(Large(), Leader(1UL), cancellationToken));

        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsFalse();
    }

    /// <summary>A snapshot the log cannot publish is refused when the copy would be marked: the copy stays complete and its metadata keeps its bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedSnapshotIsRefusedBeforeTheMark(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-oversized-begin");
        await using var log = OpenLimited(dir, 400);
        await log.OpenSeededAsync(cancellationToken);
        var before = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, Large(), 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.IsComplete).IsTrue();
        var after = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A snapshot that fails an eligibility check of the completion is refused when the copy would be marked: the copy stays complete and untouched.</summary>
    /// <param name="check">The check under test.</param>
    /// <param name="expected">The refusal the begin gives.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("other-group", "not-member")]
    [Arguments("zero-index", "not-ready")]
    [Arguments("zero-term", "not-ready")]
    [Arguments("commit-below-index", "not-ready")]
    [Arguments("below-durable-commit", "not-ready")]
    public async Task IneligibleSnapshotIsRefusedBeforeTheMark(string check, string expected, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-ineligible");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);
        var valid = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 3UL, 3UL, [], DateTime.UnixEpoch);
        var snapshot = check switch
        {
            "other-group" => valid with { GroupId = "other" },
            "zero-index" => valid with { LastIncludedIndex = 0UL, CommitIndex = 0UL },
            "zero-term" => valid with { LastIncludedTerm = 0UL },
            "commit-below-index" => valid with { CommitIndex = 2UL },
            _ => valid with { LastIncludedIndex = 1UL, CommitIndex = 1UL },
        };
        var before = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);

        var refusal = await log.BeginStateInstallAsync(Leader(1UL), 3UL, status.TopologyFingerprint, status.ConfigurationGeneration, snapshot, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(expected);
        _ = await Assert.That(log.IsComplete).IsTrue();
        var after = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>A cancellation after the snapshot is published does not fail the log: the completion runs to its end and the copy is complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationAfterPublishStillCompletes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-cancel-after");
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var faults = new StateInstallFaultHooks();
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 2UL, 2UL, [], DateTime.UnixEpoch);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, snapshot, 1UL, cancellationToken)).IsNull();
        faults.CancelOnNextFlush(cancel);

        var result = await log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancel.Token);

        _ = await Assert.That(cancel.IsCancellationRequested).IsTrue().Because("The token was cancelled after the publish.");
        _ = await Assert.That((result.Success, log.Readiness, log.IsComplete)).IsEqualTo((true, FollowerLogReadiness.Ready, true));
        var done = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((done.LastLogIndex, done.CommitIndex, done.LastAppliedIndex)).IsEqualTo((2UL, 2UL, 2UL));
    }

    /// <summary>A cancellation before the snapshot is published changes nothing: the copy stays incomplete and the log stays ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationBeforePublishChangesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-cancel-before");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 2UL, 2UL, [], DateTime.UnixEpoch);
        _ = await Assert.That(await FollowerSnapshotScenario.BeginAsync(log, snapshot, 1UL, cancellationToken)).IsNull();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(log.CompleteStateInstallAsync(snapshot, Leader(1UL), cancelled.Token));

        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(log.IsComplete).IsFalse();
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsFalse();
        var after = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((after.LastLogIndex, after.CommitIndex)).IsEqualTo((1UL, 1UL));
    }

    private static FollowerLog OpenLimited(TempDirectory dir, int maxSnapshotBytes) => new(
        dir,
        GroupId,
        GroupComposition.Create(GroupId),
        NullLogger<FollowerLog>.Instance,
        new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, MaxSnapshotBytes = maxSnapshotBytes });

    private static GroupSnapshot Large()
    {
        var now = DateTime.UtcNow;
        var outcomes = new List<GroupIdempotencyRecord>();
        for (var i = 0; i < 8; i++)
            outcomes.Add(new GroupIdempotencyRecord("client", $"operation-{i}", new byte[] { 1 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL));

        return new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, outcomes, now);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    /// <summary>Opens a log holding two committed and applied entries and marks its copy incomplete for a transfer at the given index.</summary>
    /// <param name="dir">The data directory.</param>
    /// <param name="index">The index the transfer begins at.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The open log with an incomplete copy.</returns>
    private static async Task<FollowerLog> OpenIncompleteAsync(TempDirectory dir, ulong index, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(2UL, cancellationToken);
        _ = await FollowerSnapshotScenario.BeginAsync(log, index, 1UL, cancellationToken);
        return log;
    }

    private static async Task AssertRefusedUntouchedAsync(TempDirectory dir, FollowerLog log, GroupSnapshotInstallResult result, CancellationToken cancellationToken)
    {
        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId, NoOpFollowerLogFaultHooks.Instance).SnapshotExists).IsFalse();
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);
}
