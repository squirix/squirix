using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Restoring the outcomes rebuilt from the committed log into the idempotency store, newest log index first.</summary>
public sealed class ReplicaOutcomeRestoreTests : ServerUnitTestBase
{
    /// <summary>A rebuilt outcome takes the place of the snapshot outcome with the oldest log index when the store is full.</summary>
    [Test]
    public async Task RestoredOutcomeEvictsOlderOne()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        state.RestoreFromSnapshot([Outcome("one", 1UL), Outcome("two", 2UL)], DateTime.UnixEpoch, []);

        var restored = state.RestoreOutcome(Outcome("five", 5UL), TimeSpan.Zero);

        _ = await Assert.That(restored).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(state.Lookup("client", "one", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "two", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "five", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A rebuilt outcome with a newer log index replaces the stale snapshot outcome of a reused identity.</summary>
    [Test]
    public async Task NewerOutcomeReplacesReusedIdentity()
    {
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1));
        state.RestoreFromSnapshot([Outcome("x", 1UL, 1)], DateTime.UnixEpoch, []);

        var restored = state.RestoreOutcome(Outcome("x", 5UL, 2), TimeSpan.Zero);

        _ = await Assert.That(restored).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(state.Lookup("client", "x", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([2], record.OutcomePayload.ToArray());
    }

    /// <summary>A pinned record is never evicted to make room for a rebuilt outcome.</summary>
    [Test]
    public async Task RestoredOutcomeNeverEvictsPin()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1));
        _ = state.Reserve("client", "pin", [1], GroupRecordKind.UserMutation, 9UL, 1UL, true);

        var restored = state.RestoreOutcome(Outcome("five", 5UL), TimeSpan.Zero);

        _ = await Assert.That(restored).IsEqualTo(GroupOutcomeRestoreResult.Full);
        _ = await Assert.That(state.Lookup("client", "pin", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>A rebuilt outcome never evicts an outcome with a newer log index.</summary>
    [Test]
    public async Task RestoredOutcomeNeverEvictsNewerOne()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1));

        var first = state.RestoreOutcome(Outcome("five", 5UL), TimeSpan.Zero);
        var second = state.RestoreOutcome(Outcome("four", 4UL), TimeSpan.Zero);

        _ = await Assert.That(first).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(second).IsEqualTo(GroupOutcomeRestoreResult.Full);
        _ = await Assert.That(state.Lookup("client", "five", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A rebuild that failed part-way and is started again evicts by the records of the retry, not by an order built for the failed one.</summary>
    [Test]
    public async Task RetriedRebuildEvictsOldestIndex()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        state.RestoreFromSnapshot([Outcome("one", 1UL), Outcome("two", 2UL)], DateTime.UnixEpoch, []);
        state.BeginOutcomeRebuild();
        _ = state.RestoreOutcome(Outcome("five", 5UL), TimeSpan.Zero);

        // The failed rebuild is dropped, and the store holds other records when the retry starts.
        state.RestoreFromSnapshot([Outcome("three", 3UL), Outcome("four", 4UL)], DateTime.UnixEpoch, []);
        state.BeginOutcomeRebuild();
        var restored = state.RestoreOutcome(Outcome("nine", 9UL), TimeSpan.Zero);

        _ = await Assert.That(restored).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(state.Lookup("client", "three", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "four", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>An outcome cannot be restored once the rebuild is marked done.</summary>
    [Test]
    public void RestoreAfterRebuildThrows()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        state.MarkOutcomesRebuilt();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(state, static value => _ = value.RestoreOutcome(Outcome("one", 1UL), TimeSpan.Zero));
    }

    /// <summary>Pins admitted past the capacity during the rebuild are made up for by dropping the oldest outcomes once it is done.</summary>
    [Test]
    public async Task RebuildTrimsOldestOutcomesPastCapacity()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        state.RestoreFromSnapshot([Outcome("one", 1UL), Outcome("two", 2UL)], DateTime.UnixEpoch, []);
        _ = state.Reserve("client", "pin", [1], GroupRecordKind.UserMutation, 9UL, 1UL, true);

        state.MarkOutcomesRebuilt();

        _ = await Assert.That(state.Lookup("client", "one", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "two", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "pin", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>The final trim orders the outcomes the rebuild admitted after its first eviction too.</summary>
    [Test]
    public async Task TrimSeesOutcomesAddedMidRebuild()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        state.RestoreFromSnapshot([Outcome("one", 1UL)], DateTime.UnixEpoch, []);
        _ = state.Reserve("client", "pin-a", [1], GroupRecordKind.UserMutation, 20UL, 1UL, true);
        _ = state.Reserve("client", "pin-b", [1], GroupRecordKind.UserMutation, 21UL, 1UL, true);
        state.BeginOutcomeRebuild();

        var restored = state.RestoreOutcome(Outcome("ten", 10UL), TimeSpan.Zero);
        state.MarkOutcomesRebuilt();

        _ = await Assert.That(restored).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(state.Lookup("client", "one", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "ten", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.Lookup("client", "pin-a", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(state.Lookup("client", "pin-b", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>A committed outcome is refused while the outcomes of the log are not rebuilt, and when it is not resolved.</summary>
    [Test]
    public void CommittedOutcomeRequiresRebuild()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(state, static value => value.RecordCommittedOutcome(Outcome("one", 1UL)));
        state.MarkOutcomesRebuilt();
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(state, static value => value.RecordCommittedOutcome(Outcome("one", 1UL) with { ResolvedUtc = null }));
    }

    /// <summary>The committed outcome of a pinned entry resolves its pin.</summary>
    [Test]
    public async Task CommittedOutcomeResolvesPin()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1), clock);
        _ = state.Reserve("client", "pin", [1], GroupRecordKind.UserMutation, 5UL, 1UL, true);
        state.MarkOutcomesRebuilt();

        state.RecordCommittedOutcome(DecidedAt(Outcome("pin", 5UL, 7), clock.GetUtcNow().UtcDateTime));

        _ = await Assert.That(state.Lookup("client", "pin", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([7], record.OutcomePayload.ToArray());
        _ = await Assert.That(state.HasUnresolvedThrough(5UL)).IsFalse();
    }

    /// <summary>A committed outcome never replaces a retained outcome of the same identity with the same or a newer log index.</summary>
    [Test]
    public async Task CommittedOutcomeKeepsNewerOne()
    {
        var state = new GroupIdempotencyState(2, TimeSpan.FromHours(1));
        _ = state.RestoreOutcome(Outcome("x", 9UL, 2), TimeSpan.Zero);
        state.MarkOutcomesRebuilt();

        state.RecordCommittedOutcome(Outcome("x", 4UL, 1));
        state.RecordCommittedOutcome(Outcome("x", 9UL, 3));

        _ = await Assert.That(state.Lookup("client", "x", [1], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync<byte>([2], record.OutcomePayload.ToArray());
        _ = await Assert.That(record.LogIndex).IsEqualTo(9UL);
    }

    /// <summary>A committed outcome is stored even when the store is full, and it evicts no retained outcome.</summary>
    [Test]
    public async Task CommittedOutcomeAdmitsPastCapacity()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1), clock);
        _ = state.RestoreOutcome(Outcome("one", 1UL), TimeSpan.Zero);
        state.MarkOutcomesRebuilt();

        state.RecordCommittedOutcome(DecidedAt(Outcome("two", 2UL), clock.GetUtcNow().UtcDateTime));

        _ = await Assert.That(state.Lookup("client", "one", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Lookup("client", "two", [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.Reserve("client", "three", [1], GroupRecordKind.UserMutation, 3UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
    }

    /// <summary>The retention of a committed outcome counts from its decision time, so it leaves the store when the leader's outcome does.</summary>
    [Test]
    public async Task CommittedOutcomeAgesFromDecision()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var state = new GroupIdempotencyState(4, TimeSpan.FromHours(1), clock);
        state.MarkOutcomesRebuilt();
        var now = clock.GetUtcNow().UtcDateTime;

        state.RecordCommittedOutcome(DecidedAt(Outcome("recent", 1UL), now - TimeSpan.FromMinutes(50)));
        state.RecordCommittedOutcome(DecidedAt(Outcome("ahead", 2UL), now + TimeSpan.FromMinutes(5)));
        state.RecordCommittedOutcome(DecidedAt(Outcome("expired", 3UL), now - TimeSpan.FromHours(2)));
        var fresh = (state.Lookup("client", "recent", [1], out _), state.Lookup("client", "ahead", [1], out _), state.Lookup("client", "expired", [1], out _));
        clock.Advance(TimeSpan.FromMinutes(11));
        var aged = (state.Lookup("client", "recent", [1], out _), state.Lookup("client", "ahead", [1], out _));

        _ = await Assert.That(fresh).IsEqualTo((GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Miss));
        _ = await Assert.That(aged).IsEqualTo((GroupIdempotencyLookup.Miss, GroupIdempotencyLookup.Found));
    }

    private static GroupIdempotencyRecord DecidedAt(in GroupIdempotencyRecord record, DateTime decidedUtc) => record with { CreatedUtc = decidedUtc, ResolvedUtc = decidedUtc };

    private static GroupIdempotencyRecord Outcome(string operationId, ulong logIndex, byte outcome = 200) =>
        new("client", operationId, new byte[] { 1 }, new[] { outcome }, GroupRecordKind.UserMutation, DateTime.UnixEpoch, DateTime.UnixEpoch, logIndex, 1UL);
}
