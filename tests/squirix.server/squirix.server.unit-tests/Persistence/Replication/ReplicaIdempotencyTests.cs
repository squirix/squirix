using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Idempotency reservation, retention, and truncation semantics.</summary>
public sealed class ReplicaIdempotencyTests : ServerUnitTestBase
{
    /// <summary>Mutating the caller-provided buffer after reservation or resolution does not affect the stored record.</summary>
    [Test]
    public async Task CallerMutationDoesNotCorruptStoredRecord()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        var fingerprint = new byte[] { 1, 2, 3 };
        var outcome = new byte[] { 7, 8, 9 };

        _ = state.Reserve("client", "operation", fingerprint, GroupRecordKind.UserMutation, 1UL, 1UL);
        fingerprint[0] = 0xFF;
        _ = state.TryResolve("client", "operation", outcome, 1UL, 1UL);
        outcome[0] = 0xFF;

        _ = await Assert.That(state.Lookup("client", "operation", [1, 2, 3], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7, 8, 9], record.OutcomePayload.ToArray());
        _ = await Assert.That(record.IsResolved).IsTrue();
    }

    /// <summary>Capacity never evicts an unexpired resolved outcome.</summary>
    [Test]
    public async Task CapacityDoesNotEvictUnexpiredOutcome()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(1, TimeSpan.FromMinutes(5), clock);
        _ = state.Reserve("client", "first", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = state.TryResolve("client", "first", [2], 1UL, 1UL);

        _ = await Assert.That(state.Reserve("client", "second", [3], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        clock.Advance(TimeSpan.FromMinutes(6));
        state.Expire();
        _ = await Assert.That(state.Reserve("client", "second", [3], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(state.Lookup("client", "first", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>Re-reserving the same fingerprint at new coordinates refreshes them so the record can resolve and later expire.</summary>
    [Test]
    public async Task ReReserveAtNewIndexThenResolves()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1), clock);

        // First reservation pins the only capacity slot at (4, 2).
        _ = await Assert.That(state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 4UL, 2UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);

        // The operation is re-appended at new coordinates; the record must track them, not keep stale (4, 2).
        _ = await Assert.That(state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 10UL, 3UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);

        // TryResolve against the new coordinates must succeed (previously failed on the stale coordinates).
        _ = await Assert.That(state.TryResolve("client", "operation", [7, 8], 10UL, 3UL)).IsTrue();
        _ = await Assert.That(state.Lookup("client", "operation", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7, 8], record.OutcomePayload.ToArray());

        // The resolved record no longer stays unresolved forever; after retention it expires and frees the slot.
        clock.Advance(TimeSpan.FromHours(2));
        state.Expire();
        _ = await Assert.That(state.Lookup("client", "operation", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);

        _ = await Assert.That(state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 20UL, 4UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>Repeated resolution with identical coordinates fails and keeps the original outcome and resolution timestamp.</summary>
    [Test]
    public async Task RepeatedResolveKeepsOriginalOutcome()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1), clock);
        _ = state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 4UL, 2UL);

        _ = await Assert.That(state.TryResolve("client", "operation", [7, 8], 4UL, 2UL)).IsTrue();
        var resolvedUtc = clock.GetUtcNow().UtcDateTime;

        // The second resolution attempt at the same coordinates must fail without touching the durable outcome.
        clock.Advance(TimeSpan.FromMinutes(5));
        _ = await Assert.That(state.TryResolve("client", "operation", [9], 4UL, 2UL)).IsFalse();

        _ = await Assert.That(state.Lookup("client", "operation", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7, 8], record.OutcomePayload.ToArray());
        _ = await Assert.That(record.ResolvedUtc).IsEqualTo(resolvedUtc);
    }

    /// <summary>Re-reserving a resolved record at new coordinates must not refresh them; the original outcome stays authoritative.</summary>
    [Test]
    public async Task ResolvedRecordIgnoresReReservation()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        _ = state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 4UL, 2UL);
        _ = await Assert.That(state.TryResolve("client", "operation", [7, 8], 4UL, 2UL)).IsTrue();

        // Re-reserving the same fingerprint at new coordinates must succeed without touching the resolved record's coordinates.
        _ = await Assert.That(state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 10UL, 3UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);

        // TryResolve against the new coordinates must fail because the record never moved there.
        _ = await Assert.That(state.TryResolve("client", "operation", [9], 10UL, 3UL)).IsFalse();
        _ = await Assert.That(state.Lookup("client", "operation", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.LogIndex).IsEqualTo(4UL);
        _ = await Assert.That(record.Term).IsEqualTo(2UL);
        await SequenceAssert.EqualAsync<byte>([7, 8], record.OutcomePayload.ToArray());
    }

    /// <summary>Expired snapshot outcomes are filtered at restore and do not cause capacity refusal.</summary>
    [Test]
    public async Task RestoreFromSnapshotDropsExpiredRecords()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1), clock);
        var now = clock.GetUtcNow().UtcDateTime;
        var fresh1 = new GroupIdempotencyRecord("client", "fresh1", new byte[] { 1 }, new byte[] { 11 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var fresh2 = new GroupIdempotencyRecord("client", "fresh2", new byte[] { 2 }, new byte[] { 20 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL);
        var time = now - TimeSpan.FromHours(2);
        var expired = new GroupIdempotencyRecord("client", "expired", new byte[] { 3 }, new byte[] { 30 }, GroupRecordKind.UserMutation, time, time, 3UL, 1UL);

        // Two live outcomes fit capacity 2; the third is past retention and must be dropped rather than refused.
        state.RestoreFromSnapshot(new[] { fresh1, fresh2, expired }, []);

        _ = await Assert.That(state.Lookup("client", "fresh1", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "fresh2", [2], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "expired", [3], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>RestoreFromSnapshot rejects snapshot and retained records that exceed configured capacity.</summary>
    [Test]
    public void RestoreFromSnapshotRejectsOverCapacity()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1), clock);
        var now = clock.GetUtcNow().UtcDateTime;
        var record1 = new GroupIdempotencyRecord("client", "op1", new byte[] { 1 }, new byte[] { 11 }, GroupRecordKind.UserMutation, now, now, 1UL, 1UL);
        var record2 = new GroupIdempotencyRecord("client", "op2", new byte[] { 2 }, new byte[] { 20 }, GroupRecordKind.UserMutation, now, now, 2UL, 1UL);
        var record3 = new GroupIdempotencyRecord("client", "op3", new byte[] { 3 }, new byte[] { 30 }, GroupRecordKind.UserMutation, now, now, 3UL, 1UL);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(state, new[] { record1, record2, record3 }, static (s, records) => { s.RestoreFromSnapshot(records, []); });
    }

    /// <summary>RestoreFromSnapshot rejects unresolved records that violate the snapshot contract.</summary>
    [Test]
    public void RestoreRejectsUnresolvedRecords()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        var memory = ReadOnlyMemory<byte>.Empty;
        var unresolved = new GroupIdempotencyRecord("client", "unresolved", new byte[] { 1 }, memory, GroupRecordKind.UserMutation, DateTime.UnixEpoch, null, 1UL, 1UL);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(state, unresolved, static (s, record) => { s.RestoreFromSnapshot(new[] { record }); });
    }

    /// <summary>RestoreFromSnapshot preserves a retained record when its key duplicates a snapshot outcome.</summary>
    [Test]
    public async Task RestoreRetainedDuplicateWins()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1), clock);
        _ = state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 2UL, 1UL);
        _ = state.TryResolve("client", "operation", [9], 2UL, 1UL);

        // A current, non-expired outcome so FilterExpiredSnapshot keeps the snapshot duplicate, and the test
        // genuinely exercises retained-record precedence rather than relying on the snapshot record expiring.
        var time = clock.GetUtcNow().UtcDateTime;
        var record = new GroupIdempotencyRecord("client", "operation", new byte[] { 1 }, new byte[] { 3 }, GroupRecordKind.UserMutation, time, time, 1UL, 1UL);

        var snapshotRecords = new[] { record };
        var retainedIndexes = new[] { 2UL };
        state.RestoreFromSnapshot(snapshotRecords, retainedIndexes);

        _ = await Assert.That(state.Lookup("client", "operation", [1], out var restored)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(restored.OutcomePayload.Span is [9]).IsTrue();
        _ = await Assert.That(restored.LogIndex).IsEqualTo(2UL);
    }

    /// <summary>Matching operation identity returns the outcome, and a changed fingerprint is rejected.</summary>
    [Test]
    public async Task SameOperationResolvesOrRejects()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        _ = state.Reserve("client", "operation", [1], GroupRecordKind.UserMutation, 4UL, 2UL);
        _ = state.TryResolve("client", "operation", [7, 8], 4UL, 2UL);

        _ = await Assert.That(state.Lookup("client", "operation", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7, 8], record.OutcomePayload.ToArray());
        _ = await Assert.That(state.Lookup("client", "operation", [2], out _)).IsEqualTo(GroupIdempotencyLookup.Mismatch);

        // Re-reserving the same identity with a differing fingerprint must be rejected rather than treated as idempotent.
        _ = await Assert.That(state.Reserve("client", "operation", [2], GroupRecordKind.UserMutation, 4UL, 2UL)).IsEqualTo(GroupIdempotencyReserveResult.FingerprintMismatch);
    }

    /// <summary>Durable tail truncation releases reservations carried by the removed indexes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncationReleasesPendingOnUpdate(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-replica-idempotency-truncate");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated failure after durable truncate."));

        await using var log = new FollowerLog(dir, "grp-idempotency", GroupComposition.Create("grp-idempotency"), faults);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "old"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = log.Idempotency.Reserve("client", "pending", [1], GroupRecordKind.UserMutation, 2UL, 1UL);
        faults.Arm();

        var replacement = Append(2UL, 2UL, "new");
        var appendTask = log.AppendAsync(replacement, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(appendTask);

        _ = await Assert.That(log.Idempotency.Lookup("client", "pending", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /*
     * The declared predecessor term stays 1 so a term-2 entry at index 2 conflicts with the term-1 predecessor and
     * reaches the truncation path. Do not replace this with FollowerFoundationScenario.Append, which derives the
     * predecessor term from the entry term and would be refused with LogMismatch.
     */
    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) =>
        new("leader", term, index - 1UL, index == 1UL ? 0UL : 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))));
}
