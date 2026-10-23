using System;
using System.Collections.Generic;
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
using static Squirix.Server.UnitTests.Persistence.Replication.OutcomeBytesKit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The byte bound of the group idempotency outcomes holds across a snapshot restore, a rebuild of the log, and log compaction.</summary>
public sealed class GroupOutcomeByteRestoreTests : ServerUnitTestBase
{
    private const string GroupId = "grp-outcome-bytes-restore";

    /// <summary>An age past the minimum retention.</summary>
    private static readonly TimeSpan Old = Floor + TimeSpan.FromSeconds(1);

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

    /// <summary>A rebuild read newest first keeps the newest outcomes past the minimum retention that fit, and refuses the next without evicting.</summary>
    [Test]
    public async Task RebuildKeepsNewestThatFit()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        state.BeginOutcomeRebuild();
        var results = new List<GroupOutcomeRestoreResult>();
        for (var i = OutcomesInBound + 1; i >= 1; i--)
            results.Add(state.RestoreOutcome(Outcome(clock, Id(i), Convert.ToUInt64(i), PayloadBytes), Old));

        state.MarkOutcomesRebuilt();

        await SequenceAssert.EqualAsync(
            [GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Full],
            results);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * OutcomesInBound);
    }

    /// <summary>
    /// A rebuilt outcome evicts the snapshot outcomes with older log indexes past the minimum retention to fit, but never one younger than
    /// it: a young snapshot outcome stays replayable past the bound.
    /// </summary>
    /// <param name="snapshotAgeSeconds">The age of the snapshot outcomes at the rebuild.</param>
    /// <param name="snapshotKept">Whether the oldest snapshot outcome is still replayable after the rebuild.</param>
    [Test]
    [Arguments(10, true)]
    [Arguments(90, false)]
    public async Task RebuildKeepsYoungSnapshotOutcomes(int snapshotAgeSeconds, bool snapshotKept)
    {
        var clock = new FakeTimeProvider(Epoch);
        var live = Bounded(clock);
        for (var i = 1; i <= OutcomesInBound; i++)
            Resolve(live, Id(i), Convert.ToUInt64(i), PayloadBytes);

        clock.Advance(TimeSpan.FromSeconds(snapshotAgeSeconds));
        var outcomes = live.ExportResolved(out var capturedUtc);
        var state = Bounded(clock);
        state.RestoreFromSnapshot(outcomes, capturedUtc, []);
        state.BeginOutcomeRebuild();

        var rebuilt = state.RestoreOutcome(Outcome(clock, Id(4), 4UL, PayloadBytes), TimeSpan.Zero);
        state.MarkOutcomesRebuilt();

        _ = await Assert.That(rebuilt).IsEqualTo(GroupOutcomeRestoreResult.Restored);
        _ = await Assert.That(Lookup(state, Id(1)) == GroupIdempotencyLookup.Found).IsEqualTo(snapshotKept);
        _ = await Assert.That(Lookup(state, Id(4))).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(state.OutcomeBytes).IsEqualTo(OutcomeSize * (snapshotKept ? OutcomesInBound + 1 : OutcomesInBound));
    }

    /// <summary>A rebuilt outcome older than the minimum retention that the pins leave no room for is refused before any older outcome is evicted.</summary>
    [Test]
    public async Task RebuildRefusesWhenPinsFillBound()
    {
        var clock = new FakeTimeProvider(Epoch);
        var live = Bounded(clock);
        Resolve(live, Id(1), 1UL, PayloadBytes);
        clock.Advance(Old);
        var outcomes = live.ExportResolved(out var capturedUtc);
        var state = Bounded(clock);
        state.RestoreFromSnapshot(outcomes, capturedUtc, []);
        for (var i = 0; i < OutcomesInBound; i++)
            _ = state.Reserve(Scope, Id(10 + i), [1], GroupRecordKind.UserMutation, Convert.ToUInt64(10 + i), 1UL, new GroupReserveOptions(true, PayloadBytes));

        state.BeginOutcomeRebuild();
        var rebuilt = state.RestoreOutcome(Outcome(clock, Id(2), 2UL, PayloadBytes), Old);

        _ = await Assert.That(rebuilt).IsEqualTo(GroupOutcomeRestoreResult.Full);
        _ = await Assert.That(Lookup(state, Id(1))).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>Without a byte bound set, the options derive one from the snapshot limit, and turn it off only on request.</summary>
    /// <param name="maxSnapshotBytes">The snapshot limit.</param>
    /// <param name="setBound">The byte bound set, or <see langword="null" />.</param>
    /// <param name="expected">The byte bound the log gets.</param>
    [Test]
    [Arguments(GroupSnapshotStore.DefaultMaxSnapshotBytes, null, GroupIdempotencyState.DefaultMaxOutcomeBytes)]
    [Arguments(8192, null, 6144L)]
    [Arguments(64, null, GroupIdempotencyState.DefaultMaxOutcomeBytes)]
    [Arguments(8192, long.MaxValue, long.MaxValue)]
    public async Task OptionsDeriveBoundFromSnapshot(int maxSnapshotBytes, long? setBound, long expected)
    {
        var options = new FollowerLogOptions
        {
            ShutdownTimeProvider = TimeProvider.System,
            TimeProvider = TimeProvider.System,
            MaxSnapshotBytes = maxSnapshotBytes,
            IdempotencyMaxOutcomeBytes = setBound,
        };

        _ = await Assert.That(options.ResolveIdempotencyMaxOutcomeBytes()).IsEqualTo(expected);
    }

    /// <summary>A rebuilt outcome too large for the whole byte bound is restored when it is the newest, and the read stops at the next older one.</summary>
    [Test]
    public async Task RebuildKeepsNewestOversizedAlone()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = Bounded(clock);
        state.BeginOutcomeRebuild();

        var oversized = state.RestoreOutcome(Outcome(clock, Id(2), 2UL, OversizedPayload), Old);
        var older = state.RestoreOutcome(Outcome(clock, Id(1), 1UL, PayloadBytes), Old);
        state.MarkOutcomesRebuilt();

        _ = await Assert.That((oversized, older)).IsEqualTo((GroupOutcomeRestoreResult.Restored, GroupOutcomeRestoreResult.Full));
        _ = await Assert.That(Lookup(state, Id(2))).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A byte bound set in the options must hold an outcome and stay below the snapshot limit.</summary>
    /// <param name="bound">The byte bound set.</param>
    [Test]
    [Arguments(1L)]
    [Arguments(4096L)]
    [Arguments(8192L)]
    public void OptionsBoundPastSnapshotIsRejected(long bound)
    {
        var options = new FollowerLogOptions
        {
            ShutdownTimeProvider = TimeProvider.System,
            TimeProvider = TimeProvider.System,
            MaxSnapshotBytes = 4096,
            IdempotencyMaxOutcomeBytes = bound,
        };

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static set => _ = set.ResolveIdempotencyMaxOutcomeBytes());
    }

    /// <summary>
    /// A remove-heavy load whose outcomes carry large removed values keeps compacting the group log under the byte bound; with the default
    /// bound turned off, the snapshot outgrows the limit and compaction stalls.
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
        var withinBound = true;
        var index = 0UL;
        for (var round = 0; round < rounds; round++)
        {
            for (var i = 0; i < removesPerRound; i++)
            {
                index++;
                _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", index, 1UL, "remove-" + index), cancellationToken);
                var reserved = log.Idempotency.Reserve(Scope, "remove-" + index, [1], GroupRecordKind.UserMutation, index, 1UL, new GroupReserveOptions(false, removedValueBytes));
                _ = await Assert.That(reserved).IsEqualTo(GroupIdempotencyReserveResult.Success);
                _ = log.Idempotency.TryResolve(Scope, "remove-" + index, new byte[removedValueBytes], index, 1UL);
            }

            _ = await log.AdvanceCommitAsync(index, cancellationToken);
            _ = await log.AdvanceAppliedAsync(index, cancellationToken);
            outcomes.Add(await log.CompactThroughAsync(index, cancellationToken));
            withinBound &= log.Idempotency.OutcomeBytes <= log.Idempotency.MaxOutcomeBytes;
            clock.Advance(Floor);
        }

        if (bounded)
        {
            _ = await Assert.That(outcomes.TrueForAll(static outcome => outcome == GroupCompactionOutcome.Compacted)).IsTrue();
            _ = await Assert.That(withinBound).IsTrue();
            _ = await Assert.That(log.Idempotency.Lookup(Scope, "remove-" + index, [1], out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        }
        else
        {
            _ = await Assert.That(outcomes[^1]).IsEqualTo(GroupCompactionOutcome.SnapshotTooLarge);
        }
    }
}
