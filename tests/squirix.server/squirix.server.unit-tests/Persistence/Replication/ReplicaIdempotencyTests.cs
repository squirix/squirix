using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Idempotency reservation, retention, and truncation semantics.</summary>
public sealed class ReplicaIdempotencyTests : ServerUnitTestBase
{
    /// <summary>Matching operation identity returns the outcome, and a changed fingerprint is rejected.</summary>
    [Fact]
    public void SameOperationResolvesOrRejects()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        _ = state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 4UL, 2UL);
        _ = state.TryResolve("client", "operation", new byte[] { 7, 8 }, 4UL, 2UL);

        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1 }, out var record));
        Assert.Equal(new byte[] { 7, 8 }, record.OutcomePayload.ToArray());
        Assert.Equal(GroupIdempotencyLookup.Mismatch, state.Lookup("client", "operation", new byte[] { 2 }, out _));

        // Re-reserving the same identity with a differing fingerprint must be rejected rather than treated as idempotent.
        Assert.Equal(GroupIdempotencyReserveResult.FingerprintMismatch, state.Reserve("client", "operation", new byte[] { 2 }, GroupRecordKind.UserMutation, 4UL, 2UL));
    }

    /// <summary>Capacity never evicts an unexpired resolved outcome.</summary>
    [Fact]
    public void CapacityDoesNotEvictUnexpiredOutcome()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(1, TimeSpan.FromMinutes(5), clock);
        _ = state.Reserve("client", "first", new byte[] { 1 }, GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = state.TryResolve("client", "first", new byte[] { 2 }, 1UL, 1UL);

        Assert.Equal(GroupIdempotencyReserveResult.CapacityExceeded, state.Reserve("client", "second", new byte[] { 3 }, GroupRecordKind.UserMutation, 2UL, 1UL));
        clock.Advance(TimeSpan.FromMinutes(6));
        state.Expire();
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "second", new byte[] { 3 }, GroupRecordKind.UserMutation, 2UL, 1UL));
        Assert.Equal(GroupIdempotencyLookup.Miss, state.Lookup("client", "first", new byte[] { 1 }, out _));
    }

    /// <summary>Durable tail truncation releases reservations carried by the removed indexes.</summary>
    [Fact]
    public async Task TruncationReleasesPendingOnUpdate()
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-truncate");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated failure after durable truncate."));

        await using var log = new FollowerLog(dir, "grp-idempotency", GroupComposition.Create("grp-idempotency"), faults);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "old"), DefaultCancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, DefaultCancellationToken);
        _ = log.Idempotency.Reserve("client", "pending", new byte[] { 1 }, GroupRecordKind.UserMutation, 2UL, 1UL);
        faults.Arm();

        var replacement = Append(2UL, 2UL, "new");
        var appendTask = log.AppendAsync(replacement, DefaultCancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(appendTask);

        Assert.Equal(GroupIdempotencyLookup.Miss, log.Idempotency.Lookup("client", "pending", new byte[] { 1 }, out _));
    }

    /// <summary>Re-reserving the same fingerprint at new coordinates refreshes them so the record can resolve and later expire.</summary>
    [Fact]
    public void ReReserveAtNewIndexThenResolves()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1), clock);

        // First reservation pins the only capacity slot at (4, 2).
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 4UL, 2UL));

        // The operation is re-appended at new coordinates; the record must track them, not keep stale (4, 2).
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 10UL, 3UL));

        // TryResolve against the new coordinates must succeed (previously failed on the stale coordinates).
        Assert.True(state.TryResolve("client", "operation", new byte[] { 7, 8 }, 10UL, 3UL));
        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1 }, out var record));
        Assert.Equal(new byte[] { 7, 8 }, record.OutcomePayload.ToArray());

        // The resolved record no longer stays unresolved forever; after retention it expires and frees the slot.
        clock.Advance(TimeSpan.FromHours(2));
        state.Expire();
        Assert.Equal(GroupIdempotencyLookup.Miss, state.Lookup("client", "operation", new byte[] { 1 }, out _));

        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 20UL, 4UL));
    }

    /// <summary>Re-reserving a resolved record at new coordinates must not refresh them; the original outcome stays authoritative.</summary>
    [Fact]
    public void ResolvedRecordIgnoresReReservation()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        _ = state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 4UL, 2UL);
        Assert.True(state.TryResolve("client", "operation", new byte[] { 7, 8 }, 4UL, 2UL));

        // Re-reserving the same fingerprint at new coordinates must succeed without touching the resolved record's coordinates.
        Assert.Equal(GroupIdempotencyReserveResult.Success, state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 10UL, 3UL));

        // TryResolve against the new coordinates must fail because the record never moved there.
        Assert.False(state.TryResolve("client", "operation", new byte[] { 9 }, 10UL, 3UL));
        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1 }, out var record));
        Assert.Equal(4UL, record.LogIndex);
        Assert.Equal(2UL, record.Term);
        Assert.Equal(new byte[] { 7, 8 }, record.OutcomePayload.ToArray());
    }

    /// <summary>Repeated resolution with identical coordinates fails and keeps the original outcome and resolution timestamp.</summary>
    [Fact]
    public void RepeatedResolveKeepsOriginalOutcome()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1), clock);
        _ = state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 4UL, 2UL);

        Assert.True(state.TryResolve("client", "operation", new byte[] { 7, 8 }, 4UL, 2UL));
        var resolvedUtc = clock.GetUtcNow().UtcDateTime;

        // The second resolution attempt at the same coordinates must fail without touching the durable outcome.
        clock.Advance(TimeSpan.FromMinutes(5));
        Assert.False(state.TryResolve("client", "operation", new byte[] { 9 }, 4UL, 2UL));

        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1 }, out var record));
        Assert.Equal(new byte[] { 7, 8 }, record.OutcomePayload.ToArray());
        Assert.Equal(resolvedUtc, record.ResolvedUtc);
    }

    /// <summary>Mutating the caller-provided buffer after reservation or resolution does not affect the stored record.</summary>
    [Fact]
    public void CallerMutationDoesNotCorruptStoredRecord()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        var fingerprint = new byte[] { 1, 2, 3 };
        var outcome = new byte[] { 7, 8, 9 };

        _ = state.Reserve("client", "operation", fingerprint, GroupRecordKind.UserMutation, 1UL, 1UL);
        fingerprint[0] = 0xFF;
        _ = state.TryResolve("client", "operation", outcome, 1UL, 1UL);
        outcome[0] = 0xFF;

        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1, 2, 3 }, out var record));
        Assert.Equal(new byte[] { 7, 8, 9 }, record.OutcomePayload.ToArray());
        Assert.True(record.IsResolved);
    }

    /// <summary>RestoreFromSnapshot rejects unresolved records that violate the snapshot contract.</summary>
    [Fact]
    public void RestoreRejectsUnresolvedRecords()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        var memory = ReadOnlyMemory<byte>.Empty;
        var unresolved = new GroupIdempotencyRecord("client", "unresolved", new byte[] { 1 }, memory, GroupRecordKind.UserMutation, DateTime.UnixEpoch, null, 1UL, 1UL);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(state, unresolved, static (s, record) => { s.RestoreFromSnapshot(new[] { record }); });
    }

    /// <summary>RestoreFromSnapshot rejects snapshot and retained records that exceed configured capacity.</summary>
    [Fact]
    public void RestoreFromSnapshotRejectsOverCapacity()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1), clock);
        var now = clock.GetUtcNow().UtcDateTime;
        var record1 = new GroupIdempotencyRecord("client", "op1", new byte[] { 1 }, new byte[] { 10 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var record2 = new GroupIdempotencyRecord("client", "op2", new byte[] { 2 }, new byte[] { 20 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL);
        var record3 = new GroupIdempotencyRecord("client", "op3", new byte[] { 3 }, new byte[] { 30 }, GroupRecordKind.UserMutation, now, now, 3UL, 1UL);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(state, new[] { record1, record2, record3 }, static (s, records) => { s.RestoreFromSnapshot(records, []); });
    }

    /// <summary>Expired snapshot outcomes are filtered at restore and do not cause capacity refusal.</summary>
    [Fact]
    public void RestoreFromSnapshotDropsExpiredRecords()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1), clock);
        var now = clock.GetUtcNow().UtcDateTime;
        var fresh1 = new GroupIdempotencyRecord("client", "fresh1", new byte[] { 1 }, new byte[] { 10 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var fresh2 = new GroupIdempotencyRecord("client", "fresh2", new byte[] { 2 }, new byte[] { 20 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL);
        var time = now - TimeSpan.FromHours(2);
        var expired = new GroupIdempotencyRecord("client", "expired", new byte[] { 3 }, new byte[] { 30 }, GroupRecordKind.UserMutation, time, time, 3UL, 1UL);

        // Two live outcomes fit capacity 2; the third is past retention and must be dropped rather than refused.
        state.RestoreFromSnapshot(new[] { fresh1, fresh2, expired }, []);

        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "fresh1", new byte[] { 1 }, out _));
        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "fresh2", new byte[] { 2 }, out _));
        Assert.Equal(GroupIdempotencyLookup.Miss, state.Lookup("client", "expired", new byte[] { 3 }, out _));
    }

    /// <summary>RestoreFromSnapshot preserves a retained record when its key duplicates a snapshot outcome.</summary>
    [Fact]
    public void RestoreRetainedDuplicateWins()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1), clock);
        _ = state.Reserve("client", "operation", new byte[] { 1 }, GroupRecordKind.UserMutation, 2UL, 1UL);
        _ = state.TryResolve("client", "operation", new byte[] { 9 }, 2UL, 1UL);

        // A current, non-expired outcome so FilterExpiredSnapshot keeps the snapshot duplicate, and the test
        // genuinely exercises retained-record precedence rather than relying on the snapshot record expiring.
        var time = clock.GetUtcNow().UtcDateTime;
        var record = new GroupIdempotencyRecord("client", "operation", new byte[] { 1 }, new byte[] { 3 }, GroupRecordKind.UserMutation, time, time, 1UL, 1UL);

        var snapshotRecords = new[] { record };
        var retainedIndexes = new[] { 2UL };
        state.RestoreFromSnapshot(snapshotRecords, retainedIndexes);

        Assert.Equal(GroupIdempotencyLookup.Found, state.Lookup("client", "operation", new byte[] { 1 }, out var restored));
        Assert.Equal(new byte[] { 9 }, restored.OutcomePayload.ToArray());
        Assert.Equal(2UL, restored.LogIndex);
    }

    /*
     * The declared predecessor term stays 1 so a term-2 entry at index 2 conflicts with the term-1 predecessor and
     * reaches the truncation path. Do not replace this with FollowerFoundationScenario.Append, which derives the
     * predecessor term from the entry term and would be refused with LogMismatch.
     */
    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload)
    {
        FollowerLogEntry[] array = [new(index, term, System.Text.Encoding.UTF8.GetBytes(payload))];
        return new FollowerLogAppendRequest("leader", term, index - 1UL, index == 1UL ? 0UL : 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>(array));
    }
}
