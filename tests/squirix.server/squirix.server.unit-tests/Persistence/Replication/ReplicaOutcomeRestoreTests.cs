using System;
using System.Threading.Tasks;
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

    private static GroupIdempotencyRecord Outcome(string operationId, ulong logIndex, byte outcome = 200) =>
        new("client", operationId, new byte[] { 1 }, new byte[] { outcome }, GroupRecordKind.UserMutation, DateTime.UnixEpoch, DateTime.UnixEpoch, logIndex, 1UL);
}
