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

    private const int WindowsRun = 3;

    private const int SmallCapacity = 200;

    private const int SmallOutcomeLength = 128;

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

    /// <summary>The headroom the snapshot keeps above the byte bound covers at least the skew allowance at a full steady flow of the default bound per window, before the snapshot header.</summary>
    [Test]
    public async Task SnapshotHeadroomCoversSkewAllowance()
    {
        var headroomCoversSkew = GroupIdempotencyState.OutcomeHeadroomBytes * Window.Ticks >= GroupIdempotencyState.DefaultMaxOutcomeBytes * PolicyOptions.RfIdempotencyClockSkewAllowance.Ticks;

        _ = await Assert.That(headroomCoversSkew).IsTrue();
    }

    /// <summary>The replication policy bounds a group store by its production count and byte limits.</summary>
    [Test]
    public async Task ProductionStoreUsesPolicyLimits()
    {
        var state = NewState(new FakeTimeProvider(Epoch));

        _ = await Assert.That(state.Capacity).IsEqualTo(PolicyOptions.RfIdempotencyMaxInFlightRecords);
        _ = await Assert.That(state.MaxOutcomeBytes).IsEqualTo(PolicyOptions.RfIdempotencyMaxOutcomeBytes);
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

    /// <summary>A follower whose clock runs ahead by the skew allowance replays an outcome past the operation deadline and loses it once the window less the allowance has passed.</summary>
    [Test]
    public async Task SkewedFollowerKeepsTwentySeconds()
    {
        var skew = PolicyOptions.RfIdempotencyClockSkewAllowance;
        var clock = new FakeTimeProvider(Epoch + skew);
        var state = NewState(clock);
        state.MarkOutcomesRebuilt();
        var decidedUtc = Epoch.UtcDateTime;
        var record = new GroupIdempotencyRecord(Scope, "op", new byte[] { 1 }, new byte[] { 2 }, GroupRecordKind.UserMutation, decidedUtc, decidedUtc, 1UL, 1UL);
        state.RecordCommittedOutcome(in record);

        clock.Advance(RpcAttemptTimeout.OperationDeadline);
        var atDeadline = state.Lookup(Scope, "op", [1], out _);
        clock.Advance(Window - skew + Epsilon - RpcAttemptTimeout.OperationDeadline);
        var past = state.Lookup(Scope, "op", [1], out _);

        _ = await Assert.That(atDeadline).IsEqualTo(GroupIdempotencyLookup.Found);
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

    /// <summary>A flow below the count limit of the store per window runs for many windows without a refusal or an eviction.</summary>
    [Test]
    public async Task FlowBelowCountLimitIsNeverRefused()
    {
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(SmallCapacity, Window, clock, Window, GroupIdempotencyState.DefaultMaxOutcomeBytes);

        await AssertFlowNeverRefusedAsync(state, clock, 1);
    }

    /// <summary>A flow below the byte limit of the store per window, with outcomes small enough that the bytes are what limits, runs for many windows without a refusal or an eviction.</summary>
    [Test]
    public async Task FlowBelowByteLimitIsNeverRefused()
    {
        var charge = GroupIdempotencyState.ReservationCharge(GroupRecordKind.UserMutation, Scope, "op-0000000", 1, SmallOutcomeLength);
        var clock = new FakeTimeProvider(Epoch);
        var state = new GroupIdempotencyState(PolicyOptions.RfIdempotencyMaxInFlightRecords, Window, clock, Window, SmallCapacity * charge);

        await AssertFlowNeverRefusedAsync(state, clock, SmallOutcomeLength);
    }

    private static async Task AssertFlowNeverRefusedAsync(GroupIdempotencyState state, FakeTimeProvider clock, int outcomeLength)
    {
        var outcome = new byte[outcomeLength];
        var options = new GroupReserveOptions(false, outcomeLength);
        var charge = GroupIdempotencyState.ReservationCharge(GroupRecordKind.UserMutation, Scope, "op-0000000", 1, outcomeLength);
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
                if (state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL, options) != GroupIdempotencyReserveResult.Success)
                    refused++;
                else
                    _ = state.TryResolve(Scope, id, outcome, index, 1UL);
            }

            clock.Advance(TimeSpan.FromSeconds(1));
        }

        _ = await Assert.That(perSecond).IsGreaterThan(0);
        _ = await Assert.That(refused).IsEqualTo(0);
        _ = await Assert.That(state.CapacityEvictions).IsEqualTo(0L);
    }

    private static GroupIdempotencyState NewState(FakeTimeProvider clock) =>
        new(PolicyOptions.RfIdempotencyMaxInFlightRecords, TimeSpan.FromTicks(PolicyOptions.RfIdempotencyRetentionTicks), clock, TimeSpan.FromTicks(PolicyOptions.RfIdempotencyMinRetentionTicks), PolicyOptions.RfIdempotencyMaxOutcomeBytes);
}
