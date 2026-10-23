using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Persistence.Replication.OutcomeBytesKit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The group idempotency outcomes are bounded by the bytes they take in a snapshot as well as by their count.</summary>
public sealed class GroupOutcomeByteBoundTests : ServerUnitTestBase
{
    private const string GroupId = "grp-outcome-bytes";

    /// <summary>The store charges exactly the bytes its resolved outcomes add to a published snapshot, plus the expected outcome of each pin.</summary>
    [Test]
    public async Task AccountingMatchesSnapshotEncoding()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        Resolve(state, "a", 1UL, 10);
        Resolve(state, "longer-identity", 2UL, 700);
        Resolve(state, "c", 3UL, 0);
        _ = state.Reserve(Scope, "pinned", [1, 2, 3], GroupRecordKind.UserMutation, 4UL, 1UL, new GroupReserveOptions(false, 50));

        var outcomes = state.ExportResolved(out var capturedUtc);
        var withOutcomes = GroupSnapshotStore.ComputePublishedLength(Snapshot(outcomes, capturedUtc), int.MaxValue);
        var without = GroupSnapshotStore.ComputePublishedLength(Snapshot([], capturedUtc), int.MaxValue);
        var pin = GroupSnapshotStore.OutcomeEncodedLength(Scope, "pinned", 3, 50);

        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(Convert.ToInt64(withOutcomes - without) + pin);
    }

    /// <summary>Every path that removes a record gives its bytes back: a release of a pin, a release of the tail, and expiry.</summary>
    [Test]
    public async Task RemovalsGiveBytesBack()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        Resolve(state, Id(1), 1UL, PayloadBytes);
        Resolve(state, Id(2), 2UL, PayloadBytes);
        _ = Pin(state, Id(3), 3UL, PayloadBytes);
        var full = state.OutcomeBytes;

        _ = state.TryReleaseUnresolved(Scope, Id(3), 3UL, 1UL);
        var afterPinRelease = state.OutcomeBytes;
        _ = state.ReleaseFromIndex(2UL);
        var afterTailRelease = state.OutcomeBytes;
        clock.Advance(Retention);
        state.Expire();

        _ = await Assert.That((full, afterPinRelease, afterTailRelease)).IsEqualTo((OutcomeSize * 3, OutcomeSize * 2, OutcomeSize));
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(0L);
    }

    /// <summary>A pin moved to new coordinates keeps its charge, and a pin of a kind that answers no retry is charged nothing.</summary>
    [Test]
    public async Task MovedAndExemptPinsChargeOnce()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        _ = Pin(state, Id(1), 1UL, PayloadBytes);
        _ = Pin(state, Id(1), 2UL, PayloadBytes);
        _ = state.Reserve("expire", Id(2), [1], GroupRecordKind.Expiration, 3UL, 1UL, new GroupReserveOptions(false, PayloadBytes));
        _ = state.Reserve("system", Id(3), [1], GroupRecordKind.LeaderTerm, 4UL, 1UL, GroupReserveOptions.Logged);
        var pinned = state.OutcomeBytes;
        _ = state.TryResolve(Scope, Id(1), new byte[PayloadBytes], 2UL, 1UL);

        _ = await Assert.That((pinned, state.OutcomeBytes)).IsEqualTo((OutcomeSize, OutcomeSize));
    }

    /// <summary>An applied outcome that replaces the pin of its identity is charged once.</summary>
    [Test]
    public async Task CommittedOutcomeReplacesPinCharge()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        _ = Pin(state, Id(1), 1UL, PayloadBytes);
        state.MarkOutcomesRebuilt();

        Commit(state, clock, Id(1), 1UL, TimeSpan.Zero);

        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>Operations reserved at the same time see each other's expected outcomes, so all of them resolved stay within the bound.</summary>
    [Test]
    public async Task ConcurrentPinsStayWithinBound()
    {
        const int concurrent = OutcomesInBound + 2;
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        var results = new List<GroupIdempotencyReserveResult>();
        for (var i = 1; i <= concurrent; i++)
            results.Add(Pin(state, Id(i), Convert.ToUInt64(i), PayloadBytes));

        for (var i = 1; i <= concurrent; i++)
            _ = state.TryResolve(Scope, Id(i), new byte[PayloadBytes], Convert.ToUInt64(i), 1UL);

        await SequenceAssert.EqualAsync(
            [
                GroupIdempotencyReserveResult.Success, GroupIdempotencyReserveResult.Success, GroupIdempotencyReserveResult.Success,
                GroupIdempotencyReserveResult.CapacityExceeded, GroupIdempotencyReserveResult.CapacityExceeded,
            ],
            results);
        _ = await Assert.That(state.OutcomeBytes <= state.MaxOutcomeBytes).IsTrue();
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * OutcomesInBound);
    }

    /// <summary>An outcome that does not fit the byte bound evicts the oldest outcomes past the minimum retention, oldest first, until it fits.</summary>
    [Test]
    public async Task ByteBoundEvictsOldestFirst()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        for (var i = 1; i <= OutcomesInBound; i++)
        {
            Resolve(state, Id(i), Convert.ToUInt64(i), PayloadBytes);
            clock.Advance(TimeSpan.FromSeconds(10));
        }

        clock.Advance(Floor);
        Resolve(state, Id(4), 4UL, PayloadBytes);

        _ = await Assert.That((Lookup(state, Id(1)), Lookup(state, Id(2)), Lookup(state, Id(3)), Lookup(state, Id(4))))
                        .IsEqualTo((GroupIdempotencyLookup.Miss, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found));
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * OutcomesInBound);
        _ = await Assert.That(state.CapacityEvictions).IsEqualTo(1L);
    }

    /// <summary>With every outcome younger than the minimum retention a new operation is refused for bytes, and admitted once the oldest passes it.</summary>
    [Test]
    public async Task YoungOutcomesRefuseUntilFloor()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        for (var i = 1; i <= OutcomesInBound; i++)
            Resolve(state, Id(i), Convert.ToUInt64(i), PayloadBytes);

        var refused = Pin(state, Id(4), 4UL, PayloadBytes);
        clock.Advance(Floor);
        var admitted = Pin(state, Id(4), 4UL, PayloadBytes);

        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.CapacityEvictions).IsEqualTo(1L);
    }

    /// <summary>
    /// An outcome larger than the whole bound is admitted once the older outcomes past the minimum retention made way and it is the only one
    /// charged, so a retry replays it; it is the next to go once it is past the minimum retention itself.
    /// </summary>
    [Test]
    public async Task OversizedOutcomeIsAdmittedAlone()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        Resolve(state, Id(1), 1UL, PayloadBytes);
        clock.Advance(Floor);
        Resolve(state, Id(2), 2UL, OversizedPayload);
        var retry = state.Lookup(Scope, Id(2), [1], out var replayed);

        var refused = Pin(state, Id(3), 3UL, PayloadBytes);
        clock.Advance(Floor);
        var admitted = Pin(state, Id(3), 3UL, PayloadBytes);

        _ = await Assert.That((retry, replayed.OutcomePayload.Length)).IsEqualTo((GroupIdempotencyLookup.Found, OversizedPayload));
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, Id(2))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize);
    }

    /// <summary>An applied entry trims the oldest outcomes past the minimum retention to fit the byte bound, and keeps the younger ones.</summary>
    [Test]
    public async Task FollowerTrimsBytesPastFloorOnly()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        state.MarkOutcomesRebuilt();

        Commit(state, clock, Id(1), 1UL, TimeSpan.FromMinutes(5));
        Commit(state, clock, Id(2), 2UL, TimeSpan.FromMinutes(3));
        Commit(state, clock, Id(3), 3UL, TimeSpan.FromSeconds(30));
        Commit(state, clock, Id(4), 4UL, TimeSpan.FromSeconds(20));
        var afterFour = (Lookup(state, Id(1)), Lookup(state, Id(2)), state.OutcomeBytes);
        Commit(state, clock, Id(5), 5UL, TimeSpan.FromSeconds(10));
        Commit(state, clock, Id(6), 6UL, TimeSpan.FromSeconds(5));

        _ = await Assert.That(afterFour).IsEqualTo((GroupIdempotencyLookup.Miss, GroupIdempotencyLookup.Found, OutcomeSize * OutcomesInBound));
        _ = await Assert.That(Lookup(state, Id(2))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That((Lookup(state, Id(3)), Lookup(state, Id(4)), Lookup(state, Id(5)), Lookup(state, Id(6))))
                        .IsEqualTo((GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found));
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * 4);
    }

    /// <summary>A byte bound that is not positive is rejected.</summary>
    [Test]
    public void NonPositiveBoundIsRejected()
    {
        var clock = new FakeTimeProvider(Epoch);

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(clock, static time => _ = new GroupIdempotencyState(1, Retention, time, Floor, 0L));
    }

    private static GroupSnapshot Snapshot(IReadOnlyList<GroupIdempotencyRecord> outcomes, DateTime capturedUtc) =>
        new(GroupId, new byte[] { 1, 2, 3 }, 1UL, 1UL, 4UL, 4UL, outcomes, capturedUtc);
}
