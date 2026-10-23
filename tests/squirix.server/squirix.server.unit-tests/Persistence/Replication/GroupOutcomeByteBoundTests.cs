using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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

/// <summary>The group idempotency outcomes are bounded by the bytes they take in a snapshot as well as by their count.</summary>
public sealed class GroupOutcomeByteBoundTests : ServerUnitTestBase
{
    private const string Scope = "client";

    /// <summary>The payload of a regular outcome, as the removed value of a remove.</summary>
    private const int PayloadBytes = 100;

    /// <summary>The number of regular outcomes the byte bound of the store holds.</summary>
    private const int OutcomesInBound = 3;

    /// <summary>A capacity no test reaches, so only the byte bound evicts.</summary>
    private const int RoomyCapacity = 1024;

    private const string GroupId = "grp-outcome-bytes";

    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(15);

    private static readonly TimeSpan Floor = TimeSpan.FromMinutes(1);

    private static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The snapshot bytes of one regular outcome: every test identity has the same length.</summary>
    private static readonly long OutcomeSize = GroupSnapshotStore.OutcomeEncodedLength(Scope, Id(0), 1, PayloadBytes);

    /// <summary>The store counts exactly the bytes its resolved outcomes add to a published snapshot, and none for a pin.</summary>
    [Test]
    public async Task AccountingMatchesSnapshotEncoding()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        Resolve(state, "a", 1UL, 10);
        Resolve(state, "longer-identity", 2UL, 700);
        Resolve(state, "c", 3UL, 0);
        _ = state.Reserve(Scope, "pinned", [1, 2, 3], GroupRecordKind.UserMutation, 4UL, 1UL);

        var outcomes = state.ExportResolved(out var capturedUtc);
        var withOutcomes = GroupSnapshotStore.ComputePublishedLength(Snapshot(outcomes, capturedUtc), int.MaxValue);
        var without = GroupSnapshotStore.ComputePublishedLength(Snapshot([], capturedUtc), int.MaxValue);

        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(Convert.ToInt64(withOutcomes - without));
    }

    /// <summary>Every path that removes a record gives its bytes back: a release of the tail, a release of a pin, and expiry.</summary>
    [Test]
    public async Task RemovalsGiveBytesBack()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(RoomyCapacity, Retention, clock, Floor);
        Resolve(state, Id(1), 1UL, PayloadBytes);
        Resolve(state, Id(2), 2UL, PayloadBytes);
        _ = state.Reserve(Scope, Id(3), [1], GroupRecordKind.UserMutation, 3UL, 1UL);
        var full = state.OutcomeBytes;

        _ = state.TryReleaseUnresolved(Scope, Id(3), 3UL, 1UL);
        var afterPinRelease = state.OutcomeBytes;
        _ = state.ReleaseFromIndex(2UL);
        var afterTailRelease = state.OutcomeBytes;
        clock.Advance(Retention);
        state.Expire();

        _ = await Assert.That((full, afterPinRelease, afterTailRelease)).IsEqualTo((OutcomeSize * 2, OutcomeSize * 2, OutcomeSize));
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(0L);
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

        var refused = state.Reserve(Scope, Id(4), [1], GroupRecordKind.UserMutation, 4UL, 1UL);
        clock.Advance(Floor);
        var admitted = state.Reserve(Scope, Id(4), [1], GroupRecordKind.UserMutation, 4UL, 1UL);

        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.CapacityEvictions).IsEqualTo(1L);
    }

    /// <summary>
    /// An outcome larger than the whole bound is kept, alone once the older outcomes past the minimum retention made way, so a retry
    /// replays it; it is the next to go once it is past the minimum retention itself.
    /// </summary>
    [Test]
    public async Task OversizedOutcomeIsKeptAlone()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        Resolve(state, Id(1), 1UL, PayloadBytes);
        clock.Advance(Floor);
        Resolve(state, Id(2), 2UL, Convert.ToInt32(OutcomeSize * OutcomesInBound * 2));
        var retry = state.Lookup(Scope, Id(2), [1], out var replayed);

        var refused = state.Reserve(Scope, Id(3), [1], GroupRecordKind.UserMutation, 3UL, 1UL);
        clock.Advance(Floor);
        var admitted = state.Reserve(Scope, Id(3), [1], GroupRecordKind.UserMutation, 3UL, 1UL);

        _ = await Assert.That((retry, replayed.OutcomePayload.Length)).IsEqualTo((GroupIdempotencyLookup.Found, Convert.ToInt32(OutcomeSize * OutcomesInBound * 2)));
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, Id(2))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(0L);
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

    /// <summary>A store restored from a snapshot holds the outcomes and the bytes of the store that exported it, and evicts in the same order.</summary>
    [Test]
    public async Task RestoreMatchesLiveState()
    {
        var clock = new FakeTimeProvider(Epoch);
        var live = Bounded(clock);
        for (var i = 1; i <= OutcomesInBound; i++)
        {
            Resolve(live, Id(i), Convert.ToUInt64(i), PayloadBytes);
            clock.Advance(TimeSpan.FromSeconds(10));
        }

        var outcomes = live.ExportResolved(out var capturedUtc);
        var restored = Bounded(clock);
        restored.RestoreFromSnapshot(outcomes, capturedUtc, []);
        clock.Advance(Floor);
        Resolve(live, Id(4), 4UL, PayloadBytes);
        Resolve(restored, Id(4), 4UL, PayloadBytes);

        _ = await Assert.That(restored.OutcomeBytes).IsEqualTo(live.OutcomeBytes);
        for (var i = 1; i <= OutcomesInBound + 1; i++)
            _ = await Assert.That(Lookup(restored, Id(i))).IsEqualTo(Lookup(live, Id(i)));

        _ = await Assert.That(Lookup(restored, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A rebuild read newest first keeps the newest outcomes that fit the byte bound, and refuses the first older one that does not.</summary>
    [Test]
    public async Task RebuildKeepsNewestThatFit()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        state.BeginOutcomeRebuild();
        var results = new List<GroupOutcomeRestoreResult>();
        for (var i = OutcomesInBound + 1; i >= 1; i--)
            results.Add(state.RestoreOutcome(Outcome(clock, Id(i), Convert.ToUInt64(i), PayloadBytes), TimeSpan.Zero));

        state.MarkOutcomesRebuilt();

        await SequenceAssert.EqualAsync(
            [GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Full],
            results);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * OutcomesInBound);
    }

    /// <summary>A rebuilt outcome too large for the whole byte bound is restored when it is the newest, and the read stops at the next one.</summary>
    [Test]
    public async Task RebuildKeepsNewestOversizedAlone()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        state.BeginOutcomeRebuild();

        var oversized = state.RestoreOutcome(Outcome(clock, Id(2), 2UL, Convert.ToInt32(OutcomeSize * OutcomesInBound * 2)), TimeSpan.Zero);
        var older = state.RestoreOutcome(Outcome(clock, Id(1), 1UL, PayloadBytes), TimeSpan.Zero);
        state.MarkOutcomesRebuilt();

        _ = await Assert.That((oversized, older)).IsEqualTo((GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Full));
        _ = await Assert.That(Lookup(state, Id(2))).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A byte bound that is not positive is rejected.</summary>
    [Test]
    public void NonPositiveBoundIsRejected()
    {
        var clock = new FakeTimeProvider(Epoch);

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(clock, static time => _ = new GroupIdempotencyState(1, Retention, time, Floor, 0L));
    }

    /// <summary>
    /// A remove-heavy load whose outcomes carry large removed values keeps compacting the group log under the byte bound; without the bound
    /// the snapshot outgrows its limit and compaction stalls.
    /// </summary>
    /// <param name="bounded">Whether the store bounds its outcome bytes below the snapshot limit.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task RemoveHeavyLoadKeepsCompacting(bool bounded, CancellationToken cancellationToken)
    {
        const int rounds = 8;
        const int removesPerRound = 3;
        const int removedValueBytes = 1024;
        const int maxSnapshotBytes = 8 * 1024;
        using var dir = new TempDirectory("squirix-outcome-bytes-compaction");
        var clock = new FakeTimeProvider(Epoch);
        var options = new FollowerLogOptions
        {
            ShutdownTimeProvider = TimeProvider.System,
            TimeProvider = clock,
            IdempotencyCapacity = RoomyCapacity,
            IdempotencyRetention = Retention,
            IdempotencyMinRetention = Floor,
            IdempotencyMaxOutcomeBytes = bounded ? maxSnapshotBytes / 2 : long.MaxValue,
            MaxSnapshotBytes = maxSnapshotBytes,
        };
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options);
        await log.OpenSeededAsync(cancellationToken);
        var outcomes = new List<GroupCompactionOutcome>();
        var index = 0UL;
        for (var round = 0; round < rounds; round++)
        {
            for (var i = 0; i < removesPerRound; i++)
            {
                index++;
                _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", index, 1UL, "remove-" + index), cancellationToken);
                var reserved = log.Idempotency.Reserve(Scope, "remove-" + index, [1], GroupRecordKind.UserMutation, index, 1UL);
                _ = await Assert.That(reserved).IsEqualTo(GroupIdempotencyReserveResult.Success);
                _ = log.Idempotency.TryResolve(Scope, "remove-" + index, new byte[removedValueBytes], index, 1UL);
            }

            _ = await log.AdvanceCommitAsync(index, cancellationToken);
            _ = await log.AdvanceAppliedAsync(index, cancellationToken);
            outcomes.Add(await log.CompactThroughAsync(index, cancellationToken));
            clock.Advance(Floor);
        }

        if (bounded)
        {
            _ = await Assert.That(outcomes.TrueForAll(static outcome => outcome == GroupCompactionOutcome.Compacted)).IsTrue();
            _ = await Assert.That(log.Idempotency.OutcomeBytes <= log.Idempotency.MaxOutcomeBytes).IsTrue();
            _ = await Assert.That(log.Idempotency.Lookup(Scope, "remove-" + index, [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        }
        else
        {
            _ = await Assert.That(outcomes[^1]).IsEqualTo(GroupCompactionOutcome.SnapshotTooLarge);
        }
    }

    private static GroupIdempotencyState Bounded(TimeProvider clock) => new(RoomyCapacity, Retention, clock, Floor, OutcomeSize * OutcomesInBound);

    private static string Id(int number) => "op-" + number.ToString("D2", CultureInfo.InvariantCulture);

    private static GroupSnapshot Snapshot(IReadOnlyList<GroupIdempotencyRecord> outcomes, DateTime capturedUtc) =>
        new(GroupId, new byte[] { 1, 2, 3 }, 1UL, 1UL, 4UL, 4UL, outcomes, capturedUtc);

    private static GroupIdempotencyRecord Outcome(FakeTimeProvider clock, string id, ulong index, int payloadBytes)
    {
        var resolvedUtc = clock.GetUtcNow().UtcDateTime;
        return new GroupIdempotencyRecord(Scope, id, new byte[] { 1 }, new byte[payloadBytes], GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, index, 1UL);
    }

    private static void Resolve(GroupIdempotencyState state, string id, ulong index, int payloadBytes)
    {
        _ = state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL);
        _ = state.TryResolve(Scope, id, new byte[payloadBytes], index, 1UL);
    }

    private static void Commit(GroupIdempotencyState state, FakeTimeProvider clock, string id, ulong index, TimeSpan decided)
    {
        var resolvedUtc = clock.GetUtcNow().UtcDateTime - decided;
        var record = new GroupIdempotencyRecord(Scope, id, new byte[] { 1 }, new byte[PayloadBytes], GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, index, 1UL);
        state.RecordCommittedOutcome(in record);
    }

    private static GroupIdempotencyLookup Lookup(GroupIdempotencyState state, string id) => state.Lookup(Scope, id, [1], out _);
}
