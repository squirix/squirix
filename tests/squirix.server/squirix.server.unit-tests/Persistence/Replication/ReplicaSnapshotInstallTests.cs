using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Snapshot installation behavior of a replica-group follower log.</summary>
public sealed class ReplicaSnapshotInstallTests : ServerUnitTestBase
{
    private const string GroupId = "grp-install";

    /// <summary>
    /// A durable fault during the installation log rewrite must not leave the in-memory idempotency map holding
    /// entries the installation discarded: idempotency is restored from the snapshot before the tail is re-appended,
    /// so a failed rewrite still exposes the snapshot outcomes instead of stale prior-state lookups (P09).
    /// </summary>
    [Fact]
    public async Task InstallRestoresIdempotencyFirst()
    {
        using var dir = new TempDirectory("squirix-install-ordering");
        var composition = GroupComposition.Create(GroupId);

        var faults = new ArmableFlushFaultHooks(static () => new InvalidOperationException("injected install rewrite fault"));
        await using (var log = new FollowerLog(dir, GroupId, composition, faults))
        {
            await log.OpenAsync(DefaultCancellationToken);

            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
            Assert.Equal(GroupIdempotencyReserveResult.Success, log.Idempotency.Reserve("client", "op-A", new byte[] { 1 }, GroupRecordKind.UserMutation, 1UL, 1UL));
            Assert.True(log.Idempotency.TryResolve("client", "op-A", new byte[] { 9 }, 1UL, 1UL));
            Assert.Equal(GroupIdempotencyLookup.Found, log.Idempotency.Lookup("client", "op-A", new byte[] { 1 }, out _));
            _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);

            var status = await log.GetStatusAsync(DefaultCancellationToken);
            const GroupRecordKind kind = GroupRecordKind.UserMutation;
            var outcome = new GroupIdempotencyRecord("client", "op-B", new byte[] { 2 }, new byte[] { 9 }, kind, DateTime.UtcNow, DateTime.UtcNow, 1UL, 1UL);
            var snapshot = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 1UL, 1UL, new List<GroupIdempotencyRecord> { outcome });

            faults.Arm();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidOperationException>(log.InstallSnapshotAsync(snapshot, DefaultCancellationToken));

            Assert.Equal(GroupIdempotencyLookup.Miss, log.Idempotency.Lookup("client", "op-A", new byte[] { 1 }, out _));
            Assert.Equal(GroupIdempotencyLookup.Found, log.Idempotency.Lookup("client", "op-B", new byte[] { 2 }, out _));
            Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);
        }

        // The durable outcome of the failed installation matters after a crash: reopening the same directory must
        // recover consistent watermarks and the snapshot's idempotency state, never stale prior-state entries.
        await using var reopened = new FollowerLog(dir, GroupId, composition);
        await reopened.OpenAsync(DefaultCancellationToken);
        var rs = await reopened.GetStatusAsync(DefaultCancellationToken);

        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal(1UL, rs.CommitIndex);
        Assert.True(rs.LastAppliedIndex <= rs.CommitIndex, $"Recovered applied watermark exceeds the commit watermark: {rs.LastAppliedIndex} > {rs.CommitIndex}.");
        Assert.True(rs.LastLogIndex >= rs.CommitIndex, $"Recovered log tail is shorter than the commit watermark: {rs.LastLogIndex} < {rs.CommitIndex}.");
        Assert.Equal(GroupIdempotencyLookup.Miss, reopened.Idempotency.Lookup("client", "op-A", new byte[] { 1 }, out _));
        Assert.Equal(GroupIdempotencyLookup.Found, reopened.Idempotency.Lookup("client", "op-B", new byte[] { 2 }, out var recovered));
        Assert.True(recovered.OutcomePayload.Span.SequenceEqual(new byte[] { 9 }), "The recovered op-B outcome payload diverges from the snapshot outcome.");
    }

    /// <summary>Install refuses a snapshot whose commit index falls below its included index, keeping watermarks coherent.</summary>
    [Fact]
    public async Task InstallRefusesCommitBelowIncluded()
    {
        using var dir = new TempDirectory("squirix-install-commit-below-included");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 5UL, 2UL, Array.Empty<GroupIdempotencyRecord>());

        var result = await log.InstallSnapshotAsync(malformed, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
        Assert.False(new GroupSnapshotStore(dir, GroupId).SnapshotExists);
    }

    /// <summary>Install refuses a snapshot whose LastIncludedTerm is zero, so the zero "unverifiable term" sentinel never becomes the baseline.</summary>
    [Fact]
    public async Task InstallRefusesZeroIncludedTerm()
    {
        using var dir = new TempDirectory("squirix-install-zero-term");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, 5UL, 5UL, Array.Empty<GroupIdempotencyRecord>());

        var result = await log.InstallSnapshotAsync(malformed, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
        Assert.False(new GroupSnapshotStore(dir, GroupId).SnapshotExists);
    }

    /// <summary>Install must not advance the applied watermark to the snapshot boundary while the durable log still covers unapplied committed frames.</summary>
    [Fact]
    public async Task InstallKeepsAppliedBelowBoundary()
    {
        using var dir = new TempDirectory("squirix-install-applied-watermark");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, DefaultCancellationToken);

        var snapshot = await log.CreateSnapshotAsync(3UL, DefaultCancellationToken);
        var result = await log.InstallSnapshotAsync(snapshot, DefaultCancellationToken);

        Assert.True(result.Success);
        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(0UL, status.LastAppliedIndex);
    }

    /// <summary>Install adopts the snapshot boundary as the applied watermark when the durable log does not reach it.</summary>
    [Fact]
    public async Task InstallAdoptsBoundaryAsApplied()
    {
        using var sourceDir = new TempDirectory("squirix-install-boundary-source");
        using var targetDir = new TempDirectory("squirix-install-boundary-target");
        var composition = GroupComposition.Create(GroupId);

        await using var source = new FollowerLog(sourceDir, GroupId, composition);
        await source.OpenAsync(DefaultCancellationToken);
        _ = await source.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await source.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await source.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await source.AdvanceCommitAsync(3UL, DefaultCancellationToken);
        var snapshot = await source.CreateSnapshotAsync(3UL, DefaultCancellationToken);

        await using var target = new FollowerLog(targetDir, GroupId, composition);
        await target.OpenAsync(DefaultCancellationToken);
        var result = await target.InstallSnapshotAsync(snapshot, DefaultCancellationToken);

        Assert.True(result.Success);
        var status = await target.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, status.LastAppliedIndex);
    }

    /// <summary>Install refuses a snapshot whose committed outcomes contain an unresolved record, so it never publishes poisoned state.</summary>
    [Fact]
    public async Task InstallRefusesUnresolvedOutcome()
    {
        using var dir = new TempDirectory("squirix-install-unresolved-outcome");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

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

        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 2UL, 2UL, new[] { unresolved });

        var result = await log.InstallSnapshotAsync(malformed, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);

        // The invalid snapshot must not have been published, so the next recovery is not poisoned.
        Assert.False(new GroupSnapshotStore(dir, GroupId).SnapshotExists);
    }

    /// <summary>Install refuses a resolved outcome whose log index lies beyond the snapshot boundary.</summary>
    [Fact]
    public async Task InstallRefusesOutcomeBeyondBoundary()
    {
        using var dir = new TempDirectory("squirix-install-outcome-beyond-boundary");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, DefaultCancellationToken);

        var now = DateTime.UtcNow;
        var outcome = new GroupIdempotencyRecord("client", "operation-1", new byte[] { 1, 2, 3 }, new byte[] { 9 }, GroupRecordKind.UserMutation, now, now, 4UL, 1UL);
        var malformed = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 3UL, 3UL, new[] { outcome });

        var result = await log.InstallSnapshotAsync(malformed, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
        Assert.False(new GroupSnapshotStore(dir, GroupId).SnapshotExists);
    }

    /// <summary>
    /// An installation whose combined idempotency restore exceeds the configured capacity is refused before
    /// publication, so no snapshot or metadata becomes durable ahead of the refused restore.
    /// </summary>
    [Fact]
    public async Task CapacityRefusalSkipsSnapshotPublish()
    {
        using var dir = new TempDirectory("squirix-install-capacity");
        var composition = GroupComposition.Create(GroupId);
        var options = new FollowerLogOptions { IdempotencyCapacity = 2 };

        await using var log = new FollowerLog(dir, GroupId, composition, options);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, "b"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(3UL, "c"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, DefaultCancellationToken);

        var status = await log.GetStatusAsync(DefaultCancellationToken);
        var now = DateTime.UtcNow;
        var outcomes = new List<GroupIdempotencyRecord>
        {
            new("client", "operation-1", new byte[] { 1 }, new byte[] { 9 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL),
            new("client", "operation-2", new byte[] { 2 }, new byte[] { 9 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL),
            new("client", "operation-3", new byte[] { 3 }, new byte[] { 9 }, GroupRecordKind.UserMutation, now, now, 3UL, 1UL),
        };
        var oversized = new GroupSnapshot(GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, 3UL, 3UL, outcomes);

        var result = await log.InstallSnapshotAsync(oversized, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, result.RefusalCode);
        Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);

        // The capacity refusal ran before any durable write: nothing was published.
        Assert.False(new GroupSnapshotStore(dir, GroupId).SnapshotExists);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => Append(index, 1UL, payload);

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => FollowerFoundationScenario.Append("leader", index, term, payload);
}
