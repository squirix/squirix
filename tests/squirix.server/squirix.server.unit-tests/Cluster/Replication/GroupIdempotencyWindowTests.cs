using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Replication;
using Squirix.Transport.Grpc;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The group idempotency store keeps an outcome for the retry window of the replication policy and for no longer, and a flow below its limits is never refused.</summary>
[Immutable]
public sealed class GroupIdempotencyWindowTests
{
    private const string Scope = "client";

    private const int WindowsRun = 10;

    private const double LoadShare = 0.9;

    private static readonly TimeSpan Window = PolicyOptions.RfIdempotencyRetryWindow;

    private static readonly TimeSpan Epsilon = TimeSpan.FromMilliseconds(1);

    private static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The window covers the SDK operation deadline plus the clock skew allowance, and both retentions of the policy are the window.</summary>
    [Test]
    public async Task WindowCoversOperationDeadlineAndSkew()
    {
        _ = await Assert.That(RpcAttemptTimeout.OperationDeadline + PolicyOptions.RfIdempotencyClockSkewAllowance <= Window).IsTrue();
        _ = await Assert.That(PolicyOptions.RfIdempotencyRetentionTicks).IsEqualTo(Window.Ticks);
        _ = await Assert.That(PolicyOptions.RfIdempotencyMinRetentionTicks).IsEqualTo(Window.Ticks);
    }

    /// <summary>The headroom the snapshot keeps above the byte bound covers the skew allowance at the default bound and window.</summary>
    [Test]
    public async Task SnapshotHeadroomCoversSkewAllowance()
    {
        var bytesPerSecond = Convert.ToDouble(GroupIdempotencyState.DefaultMaxOutcomeBytes, CultureInfo.InvariantCulture) / Window.TotalSeconds;
        var covered = TimeSpan.FromSeconds(GroupIdempotencyState.OutcomeHeadroomBytes / bytesPerSecond);

        _ = await Assert.That(covered >= PolicyOptions.RfIdempotencyClockSkewAllowance).IsTrue();
    }

    /// <summary>A leader outcome replays just inside the window and is gone just past it.</summary>
    [Test]
    public async Task LeaderOutcomeLivesForWindow()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = NewState(clock);
        _ = state.Reserve(Scope, "op", [1], GroupRecordKind.UserMutation, 1UL, 1UL, GroupReserveKit.Unlogged);
        _ = state.TryResolve(Scope, "op", [2], 1UL, 1UL);

        clock.Advance(Window - Epsilon);
        var inside = state.Lookup(Scope, "op", [1], out _);
        clock.Advance(Epsilon + Epsilon);
        var past = state.Lookup(Scope, "op", [1], out _);

        _ = await Assert.That(inside).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(past).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A follower ages an applied outcome from the leader decision time: it replays just inside the window and is gone just past it.</summary>
    [Test]
    public async Task FollowerOutcomeLivesForWindow()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = NewState(clock);
        state.MarkOutcomesRebuilt();
        var decidedUtc = clock.GetUtcNow().UtcDateTime;
        var record = new GroupIdempotencyRecord(Scope, "op", new byte[] { 1 }, new byte[] { 2 }, GroupRecordKind.UserMutation, decidedUtc, decidedUtc, 1UL, 1UL);
        state.RecordCommittedOutcome(in record);

        clock.Advance(Window - Epsilon);
        var inside = state.Lookup(Scope, "op", [1], out _);
        clock.Advance(Epsilon + Epsilon);
        var past = state.Lookup(Scope, "op", [1], out _);

        _ = await Assert.That(inside).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(past).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>An outcome restored from a snapshot keeps the rest of its window from the capture age: replayable just inside, gone just past.</summary>
    [Test]
    public async Task RestoredOutcomeLivesForRestOfWindow()
    {
        var leader = new FakeTimeProvider(Epoch);
        var source = NewState(leader);
        _ = source.Reserve(Scope, "op", [1], GroupRecordKind.UserMutation, 1UL, 1UL, GroupReserveKit.Unlogged);
        _ = source.TryResolve(Scope, "op", [2], 1UL, 1UL);
        var ageAtCapture = Window / 3;
        leader.Advance(ageAtCapture);
        var outcomes = source.ExportResolved(out var capturedUtc);

        var follower = new FakeTimeProvider(Epoch + ageAtCapture);
        var target = NewState(follower);
        target.RestoreFromSnapshot(outcomes, capturedUtc, []);
        follower.Advance(Window - ageAtCapture - Epsilon);
        var inside = target.Lookup(Scope, "op", [1], out _);
        follower.Advance(Epsilon + Epsilon);
        var past = target.Lookup(Scope, "op", [1], out _);

        _ = await Assert.That(inside).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(past).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A flow below the capacity of the store per window runs for many windows without a refusal or an eviction.</summary>
    [Test]
    public async Task FlowBelowLimitsIsNeverRefused()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = NewState(clock);
        var charge = GroupIdempotencyState.ReservationCharge(GroupRecordKind.UserMutation, Scope, "op-0000000", 1, 1);
        var perWindow = Math.Min(state.Capacity, state.MaxOutcomeBytes / charge);
        var perSecond = Convert.ToInt32(Math.Floor(LoadShare * perWindow / Window.TotalSeconds));
        var seconds = Convert.ToInt32(WindowsRun * Window.TotalSeconds);
        var refused = 0;
        var index = 0UL;

        for (var second = 0; second < seconds; second++)
        {
            for (var i = 0; i < perSecond; i++)
            {
                index++;
                var id = "op-" + index.ToString("D7", CultureInfo.InvariantCulture);
                if (state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL, GroupReserveKit.Unlogged) != GroupIdempotencyReserveResult.Success)
                    refused++;
                else
                    _ = state.TryResolve(Scope, id, [2], index, 1UL);
            }

            clock.Advance(TimeSpan.FromSeconds(1));
        }

        _ = await Assert.That(refused).IsEqualTo(0);
        _ = await Assert.That(state.CapacityEvictions).IsEqualTo(0L);
    }

    private static GroupIdempotencyState NewState(FakeTimeProvider clock) =>
        new(PolicyOptions.RfIdempotencyMaxInFlightRecords, TimeSpan.FromTicks(PolicyOptions.RfIdempotencyRetentionTicks), clock, TimeSpan.FromTicks(PolicyOptions.RfIdempotencyMinRetentionTicks), PolicyOptions.RfIdempotencyMaxOutcomeBytes);
}
