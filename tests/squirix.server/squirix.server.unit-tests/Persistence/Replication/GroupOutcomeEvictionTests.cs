using System;
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

/// <summary>A full group idempotency table gives up its oldest resolved outcome, but never one younger than the minimum retention.</summary>
public sealed class GroupOutcomeEvictionTests : ServerUnitTestBase
{
    private const string Scope = "client";

    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(15);

    private static readonly TimeSpan Floor = TimeSpan.FromMinutes(1);

    private static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>At capacity with outcomes past the minimum retention, a new operation is admitted and the oldest outcome is the one that goes.</summary>
    [Test]
    public async Task OldestOutcomeGivesWayAtCapacity()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        Resolve(state, "first", 1UL);
        clock.Advance(TimeSpan.FromSeconds(30));
        Resolve(state, "second", 2UL);
        clock.Advance(TimeSpan.FromSeconds(40));

        var admitted = state.Reserve(Scope, "third", [1], GroupRecordKind.UserMutation, 3UL, 1UL);

        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, "first")).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(state, "second")).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(state, "third")).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>With every outcome younger than the minimum retention the operation is refused, and admitted once the oldest passes it.</summary>
    [Test]
    public async Task YoungOutcomesRefuseUntilFloor()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        Resolve(state, "first", 1UL);
        clock.Advance(TimeSpan.FromSeconds(10));
        Resolve(state, "second", 2UL);
        clock.Advance(TimeSpan.FromSeconds(49));

        var refused = state.Reserve(Scope, "third", [1], GroupRecordKind.UserMutation, 3UL, 1UL);
        clock.Advance(TimeSpan.FromSeconds(1));
        var admitted = state.Reserve(Scope, "third", [1], GroupRecordKind.UserMutation, 3UL, 1UL);

        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, "first")).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(state, "second")).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A table full of unresolved records refuses a new operation however old they are.</summary>
    [Test]
    public async Task PinsAreNeverEvicted()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        _ = state.Reserve(Scope, "first", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = state.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);
        clock.Advance(Retention - TimeSpan.FromSeconds(1));

        var refused = state.Reserve(Scope, "third", [1], GroupRecordKind.UserMutation, 3UL, 1UL);

        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(Lookup(state, "first")).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(Lookup(state, "second")).IsEqualTo(GroupIdempotencyLookup.Unresolved);
    }

    /// <summary>A retry inside the minimum retention still replays its outcome while newer operations are admitted around it.</summary>
    [Test]
    public async Task RetryInsideFloorStillReplays()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        Resolve(state, "old", 1UL);
        clock.Advance(TimeSpan.FromSeconds(70));
        Resolve(state, "recent", 2UL);
        clock.Advance(TimeSpan.FromSeconds(10));

        var admitted = state.Reserve(Scope, "newer", [1], GroupRecordKind.UserMutation, 3UL, 1UL);
        var retry = state.Lookup(Scope, "recent", [1], out var record);

        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(retry).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.LogIndex).IsEqualTo(2UL);
        _ = await Assert.That(state.Reserve(Scope, "recent", [1], GroupRecordKind.UserMutation, 2UL, 1UL)).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>At a steady rate within the capacity per minimum retention every operation is admitted; with the minimum equal to the retention the table fills.</summary>
    /// <param name="floorSeconds">The minimum retention in seconds.</param>
    /// <param name="admittedAll">Whether every write is admitted.</param>
    [Test]
    [Arguments(60, true)]
    [Arguments(900, false)]
    public async Task SustainedRateIsAdmittedOnlyWithFloor(int floorSeconds, bool admittedAll)
    {
        const int Capacity = 64;
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(Capacity, Retention, clock, TimeSpan.FromSeconds(floorSeconds));
        var firstRefused = -1;
        for (var i = 0; i < Capacity * 10 && firstRefused < 0; i++)
        {
            var id = $"op{i}";
            var index = Convert.ToUInt64(i + 1);
            if (state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL) != GroupIdempotencyReserveResult.Success)
            {
                firstRefused = i;
                continue;
            }

            _ = state.TryResolve(Scope, id, [2], index, 1UL);
            clock.Advance(TimeSpan.FromSeconds(1));
        }

        _ = await Assert.That(firstRefused < 0).IsEqualTo(admittedAll);
        _ = await Assert.That(admittedAll || firstRefused == Capacity).IsTrue();
    }

    /// <summary>A follower trims the oldest outcomes past the minimum retention above the capacity and keeps the ones inside it.</summary>
    [Test]
    public async Task FollowerTrimsOutcomesPastFloorOnly()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        state.MarkOutcomesRebuilt();

        Commit(state, clock, "a", 1UL, TimeSpan.FromMinutes(5));
        Commit(state, clock, "b", 2UL, TimeSpan.FromMinutes(3));
        Commit(state, clock, "c", 3UL, TimeSpan.FromSeconds(30));
        var afterThree = (Lookup(state, "a"), Lookup(state, "b"), Lookup(state, "c"));
        Commit(state, clock, "d", 4UL, TimeSpan.FromSeconds(10));
        var afterFour = (Lookup(state, "b"), Lookup(state, "c"), Lookup(state, "d"));
        Commit(state, clock, "e", 5UL, TimeSpan.FromSeconds(5));

        _ = await Assert.That(afterThree).IsEqualTo((GroupIdempotencyLookup.Miss, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found));
        _ = await Assert.That(afterFour).IsEqualTo((GroupIdempotencyLookup.Miss, GroupIdempotencyLookup.Found, GroupIdempotencyLookup.Found));
        _ = await Assert.That(Lookup(state, "c")).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(state, "d")).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(state, "e")).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A committed outcome that is itself past the minimum retention is kept, never evicted by its own trim.</summary>
    [Test]
    public async Task CommittedOutcomeIsNeverRefused()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(1, Retention, clock, Floor);
        state.MarkOutcomesRebuilt();

        Commit(state, clock, "young", 1UL, TimeSpan.FromSeconds(5));
        Commit(state, clock, "old", 2UL, TimeSpan.FromMinutes(5));

        _ = await Assert.That(Lookup(state, "old")).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(state, "young")).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>Eviction follows the age of an outcome, not its log index: an older outcome with a higher log index goes first.</summary>
    [Test]
    public async Task EvictionFollowsAgeNotLogIndex()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(2, Retention, clock, Floor);
        Resolve(state, "older", 9UL);
        clock.Advance(TimeSpan.FromSeconds(10));
        Resolve(state, "newer", 1UL);
        clock.Advance(TimeSpan.FromSeconds(60));

        var admitted = state.Reserve(Scope, "third", [1], GroupRecordKind.UserMutation, 10UL, 1UL);

        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, "older")).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(state, "newer")).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A table holding more records than its capacity admits one write per eviction and does not grow.</summary>
    [Test]
    public async Task OverfullTableEvictsOnePerAdmission()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(1, Retention, clock, Floor);
        state.MarkOutcomesRebuilt();
        Commit(state, clock, "a", 1UL, TimeSpan.FromSeconds(30));
        Commit(state, clock, "b", 2UL, TimeSpan.FromSeconds(20));
        Commit(state, clock, "c", 3UL, TimeSpan.FromSeconds(10));
        clock.Advance(TimeSpan.FromSeconds(40));

        var first = state.Reserve(Scope, "d", [1], GroupRecordKind.UserMutation, 4UL, 1UL);
        var second = state.Reserve(Scope, "e", [1], GroupRecordKind.UserMutation, 5UL, 1UL);
        var third = state.Reserve(Scope, "f", [1], GroupRecordKind.UserMutation, 6UL, 1UL);

        _ = await Assert.That(first).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(second).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(third).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(Lookup(state, "a")).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(state, "b")).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(state, "c")).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>With unbounded retention and a finite minimum retention a full table still gives up its oldest outcome.</summary>
    [Test]
    public async Task UnboundedRetentionStillEvictsPastFloor()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(1, TimeSpan.MaxValue, clock, Floor);
        Resolve(state, "first", 1UL);

        var refused = state.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);
        clock.Advance(Floor);
        var admitted = state.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);

        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(Lookup(state, "first")).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A minimum retention that is negative or above the retention is rejected.</summary>
    [Test]
    public void InvalidMinimumRetentionIsRejected()
    {
        var clock = new FakeTimeProvider(Epoch);

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(clock, static time => _ = new GroupIdempotencyState(1, Retention, time, Retention + TimeSpan.FromTicks(1)));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(clock, static time => _ = new GroupIdempotencyState(1, Retention, time, TimeSpan.FromTicks(-1)));
    }

    /// <summary>The minimum retention defaults to the retention through the options, so a full table refuses until the outcome expires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OptionsDefaultFloorToRetention(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-outcome-eviction-default");
        var clock = new FakeTimeProvider(Epoch);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = clock, IdempotencyCapacity = 1, IdempotencyRetention = Retention };
        await using var log = new FollowerLog(dir, "group-eviction", GroupComposition.Create("group-eviction"), NullLogger<FollowerLog>.Instance, options);
        await log.OpenSeededAsync(cancellationToken);
        Resolve(log.Idempotency, "first", 1UL);
        clock.Advance(Retention - TimeSpan.FromSeconds(1));

        var refused = log.Idempotency.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);
        clock.Advance(TimeSpan.FromSeconds(1));
        var admitted = log.Idempotency.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);

        _ = await Assert.That(options.IdempotencyMinRetention).IsNull();
        _ = await Assert.That(refused).IsEqualTo(GroupIdempotencyReserveResult.CapacityExceeded);
        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>A minimum retention set in the options reaches the table of the follower log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OptionsFloorReachesTable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-outcome-eviction-floor");
        var clock = new FakeTimeProvider(Epoch);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = clock, IdempotencyCapacity = 1, IdempotencyRetention = Retention, IdempotencyMinRetention = Floor };
        await using var log = new FollowerLog(dir, "group-eviction", GroupComposition.Create("group-eviction"), NullLogger<FollowerLog>.Instance, options);
        await log.OpenSeededAsync(cancellationToken);
        Resolve(log.Idempotency, "first", 1UL);
        clock.Advance(Floor);

        var admitted = log.Idempotency.Reserve(Scope, "second", [1], GroupRecordKind.UserMutation, 2UL, 1UL);

        _ = await Assert.That(admitted).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    private static void Resolve(GroupIdempotencyState state, string id, ulong index)
    {
        _ = state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL);
        _ = state.TryResolve(Scope, id, [2], index, 1UL);
    }

    private static void Commit(GroupIdempotencyState state, FakeTimeProvider clock, string id, ulong index, TimeSpan decided)
    {
        var resolvedUtc = clock.GetUtcNow().UtcDateTime - decided;
        var record = new GroupIdempotencyRecord(Scope, id, new byte[] { 1 }, new byte[] { 2 }, GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, index, 1UL);
        state.RecordCommittedOutcome(in record);
    }

    private static GroupIdempotencyLookup Lookup(GroupIdempotencyState state, string id) => state.Lookup(Scope, id, [1], out _);
}
