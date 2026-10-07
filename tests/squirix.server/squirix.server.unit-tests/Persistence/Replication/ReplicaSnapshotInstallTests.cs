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

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Snapshot installation behavior of a replica-group follower log.</summary>
public sealed class ReplicaSnapshotInstallTests : ServerUnitTestBase
{
    private const string GroupId = "grp-install";

    /// <summary>
    /// An installation whose combined idempotency restore exceeds the configured capacity is refused before
    /// publication, so no snapshot or metadata becomes durable ahead of the refused restore.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CapacityRefusalSkipsSnapshotPublish(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-capacity");
        var composition = GroupComposition.Create(GroupId);
        var options = new FollowerLogOptions { IdempotencyCapacity = 2 };

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, options);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        var now = DateTime.UtcNow;
        var outcomes = new List<GroupIdempotencyRecord>
        {
            new("client", "operation-1", new byte[] { 1 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL),
            new("client", "operation-2", new byte[] { 2 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL),
            new("client", "operation-3", new byte[] { 3 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 3UL, 1UL),
        };
        var oversized = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 3UL, 3UL, outcomes, now);

        var result = await log.InstallSnapshotAsync(oversized, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);

        // The capacity refusal ran before any durable write: nothing was published.
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>Install adopts the snapshot boundary as the applied watermark when the durable log does not reach it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallAdoptsBoundaryAsApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-boundary-source");
        using var dir2 = new TempDirectory("squirix-install-boundary-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await source.AdvanceCommitAsync(3UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, dir, 3UL, cancellationToken);

        await using var target = new FollowerLog(dir2, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await target.OpenAsync(cancellationToken);
        var result = await target.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        var status = await target.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(3UL);
    }

    /// <summary>Install moves the applied watermark to the snapshot boundary even when the log held the covered entries unapplied: the rewrite drops their frames, so none is left to apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRaisesAppliedToBoundary(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-applied-watermark");
        using var sourceDir = new TempDirectory("squirix-install-applied-watermark-source");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await source.AdvanceCommitAsync(3UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, 3UL, cancellationToken);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);

        var result = await log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That((await log.GetCommittedEntriesAsync(0UL, 10, cancellationToken)).Count).IsEqualTo(0);
    }

    /// <summary>Installing a snapshot releases an unresolved reservation it did not export from the covered prefix, and keeps the resolved outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallReleasesPrefixReservation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-install");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.Reserve("client", "kept", [2], GroupRecordKind.UserMutation, 1UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(log.Idempotency.TryResolve("client", "kept", [3], 1UL, 1UL)).IsTrue();

        // Compaction refuses a prefix with an unresolved outcome, so only an installed snapshot can drop one; this one exports just the resolved outcome.
        var now = DateTime.UtcNow;
        var kept = new GroupIdempotencyRecord("client", "kept", new byte[] { 2 }, new byte[] { 3 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, [kept], now);
        var install = await log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);
        _ = await Assert.That(install.Success).IsTrue();

        _ = await Assert.That(log.Idempotency.Lookup("client", "orphan", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(log.Idempotency.Lookup("client", "kept", [2], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.IsResolved).IsTrue();
        _ = await Assert.That(log.Idempotency.Reserve("client", "orphan", [1], GroupRecordKind.UserMutation, 3UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>Install durably persists a higher leader term before publishing the snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallPersistsHigherLeaderTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-higher-term");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance))
        {
            await log.OpenAsync(cancellationToken);
            var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

            var result = await log.InstallSnapshotAsync(snapshot, 5UL, cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(5UL);
        }

        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(5UL);
    }

    /// <summary>Install refuses a snapshot whose commit index falls below its included index, keeping watermarks coherent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesCommitBelowIncluded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-commit-below-included");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 2UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.InstallSnapshotAsync(malformed, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>Install refuses a resolved outcome whose log index lies beyond the snapshot boundary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesOutcomeBeyondBoundary(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-outcome-beyond-boundary");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        var now = DateTime.UtcNow;
        var outcome = new GroupIdempotencyRecord("client", "operation-1", new byte[] { 1, 2, 3 }, new byte[] { 8 }, GroupRecordKind.UserMutation, now, now, 4UL, 1UL);
        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 3UL, 3UL, new[] { outcome }, now);

        var result = await log.InstallSnapshotAsync(malformed, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>Install refuses a snapshot from a deposed leader without touching durable state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesStaleLeaderTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-stale-term");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
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

        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, 2UL, 2UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>Install refuses a snapshot whose committed outcomes contain an unresolved record, so it never publishes poisoned state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesUnresolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-unresolved-outcome");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

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

        var result = await log.InstallSnapshotAsync(malformed, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);

        // The invalid snapshot must not have been published, so the next recovery is not poisoned.
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>Install refuses a snapshot whose LastIncludedTerm is zero, so the zero "unverifiable term" sentinel never becomes the baseline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRefusesZeroIncludedTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-zero-term");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, 5UL, 5UL, Array.Empty<GroupIdempotencyRecord>(), DateTime.UnixEpoch);

        var result = await log.InstallSnapshotAsync(malformed, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(new GroupSnapshotStore(dir, GroupId).SnapshotExists).IsFalse();
    }

    /// <summary>
    /// A durable fault during the installation log rewrite must not leave the in-memory idempotency map holding
    /// entries the installation discarded: idempotency is restored from the snapshot before the tail is re-appended,
    /// so a failed rewrite still exposes the snapshot outcomes instead of stale prior-state lookups (P09).
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRestoresIdempotencyFirst(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-install-ordering");
        var composition = GroupComposition.Create(GroupId);

        var faults = new ArmableFlushFaultHooks(static () => new InvalidOperationException("injected install rewrite fault"));
        await using (var log = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance, faults))
        {
            await log.OpenAsync(cancellationToken);

            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await Assert.That(log.Idempotency.Reserve("client", "op-A", [1], GroupRecordKind.UserMutation, 1UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
            _ = await Assert.That(log.Idempotency.TryResolve("client", "op-A", [9], 1UL, 1UL)).IsTrue();
            _ = await Assert.That(log.Idempotency.Lookup("client", "op-A", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

            var status = await log.GetStatusAsync(cancellationToken);
            const GroupRecordKind kind = GroupRecordKind.UserMutation;
            var outcome = new GroupIdempotencyRecord("client", "op-B", new byte[] { 2 }, new byte[] { 8 }, kind, DateTime.UtcNow, DateTime.UtcNow, 1UL, 1UL);
            var snapshot = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 1UL, 1UL, new List<GroupIdempotencyRecord> { outcome }, outcome.ResolvedUtc!.Value);

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidOperationException>(log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken));

            _ = await Assert.That(log.Idempotency.Lookup("client", "op-A", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
            _ = await Assert.That(log.Idempotency.Lookup("client", "op-B", [2], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        // The durable outcome of the failed installation matters after a crash: reopening the same directory must
        // recover consistent watermarks and the snapshot's idempotency state, never stale prior-state entries.
        await using var reopened = new FollowerLog(dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
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

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);
}
