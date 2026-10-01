using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Group idempotency outcomes age on one clock: a restoring node never compares its clock with the capturing node's.</summary>
[Immutable]
public sealed class GroupIdempotencyClockTests : ServerUnitTestBase
{
    private const string GroupId = "group-clock";

    private static readonly TimeSpan Retention = TimeSpan.FromHours(1);

    private static readonly TimeSpan AgeAtCapture = TimeSpan.FromMinutes(10);

    private static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>A node whose clock is hours ahead of or behind the capturing node keeps an outcome for exactly the rest of its window.</summary>
    /// <param name="skewHours">How far the restoring node's clock is from the capturing node's, in hours.</param>
    [Test]
    [Arguments(3)]
    [Arguments(-3)]
    public async Task RestoredAgeIgnoresNodeSkew(int skewHours)
    {
        var leader = new FakeTimeProvider(Epoch);
        var source = new GroupIdempotencyState(4, Retention, leader);
        Resolve(source);
        leader.Advance(AgeAtCapture);
        var outcomes = source.ExportResolved(out var capturedUtc);

        var follower = new FakeTimeProvider(Epoch + AgeAtCapture + TimeSpan.FromHours(skewHours));
        var target = new GroupIdempotencyState(4, Retention, follower);
        target.RestoreFromSnapshot(outcomes, capturedUtc);
        var restored = IsRetained(target);
        follower.Advance(Retention - AgeAtCapture - TimeSpan.FromSeconds(1));
        var lastSecond = IsRetained(target);
        follower.Advance(TimeSpan.FromSeconds(1));
        var expired = !IsRetained(target);

        _ = await Assert.That(restored).IsTrue();
        _ = await Assert.That(lastSecond).IsTrue();
        _ = await Assert.That(expired).IsTrue();
    }

    /// <summary>
    /// A follower whose clock is hours ahead installs a leader snapshot, restarts from it, and still keeps the outcome for the rest of
    /// its window.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkewedFollowerKeepsInstalledOutcome(CancellationToken cancellationToken)
    {
        using var sourceDir = new TempDirectory("squirix-idempotency-clock-source");
        using var targetDir = new TempDirectory("squirix-idempotency-clock-target");
        var composition = GroupComposition.Create(GroupId);
        var leader = new FakeTimeProvider(Epoch);
        var follower = new FakeTimeProvider(Epoch + AgeAtCapture + TimeSpan.FromHours(3));

        await using var source = new FollowerLog(sourceDir, GroupId, composition, NullLogger<FollowerLog>.Instance, Options(leader));
        await source.OpenAsync(cancellationToken);
        _ = await source.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
        Resolve(source.Idempotency);
        leader.Advance(AgeAtCapture);
        var snapshot = await source.CreateSnapshotAsync(1UL, cancellationToken);

        bool installed;
        await using (var target = new FollowerLog(targetDir, GroupId, composition, NullLogger<FollowerLog>.Instance, Options(follower)))
        {
            await target.OpenAsync(cancellationToken);
            _ = await Assert.That((await target.InstallSnapshotAsync(snapshot, 1UL, cancellationToken)).Success).IsTrue();
            installed = IsRetained(target.Idempotency);
        }

        await using var reopened = new FollowerLog(targetDir, GroupId, composition, NullLogger<FollowerLog>.Instance, Options(follower));
        await reopened.OpenAsync(cancellationToken);
        var restarted = IsRetained(reopened.Idempotency);
        follower.Advance(Retention - AgeAtCapture);
        var expired = !IsRetained(reopened.Idempotency);

        _ = await Assert.That(installed).IsTrue();
        _ = await Assert.That(restarted).IsTrue();
        _ = await Assert.That(expired).IsTrue();
    }

    /// <summary>A snapshot whose outcome is resolved after the capture time is never published: its age would be negative.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutcomeAfterCaptureIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-idempotency-clock-refused");
        await using (var seed = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance))
            await seed.OpenAsync(cancellationToken);

        var resolvedUtc = Epoch.UtcDateTime;
        var outcome = new GroupIdempotencyRecord("cache", "op", new byte[] { 201 }, new byte[] { 209 }, GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, 1UL, 1UL);
        var snapshot = new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 1UL, 1UL, 1UL, [outcome], resolvedUtc - TimeSpan.FromMilliseconds(1));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(new GroupSnapshotStore(dir, GroupId).PublishAsync(snapshot, cancellationToken));
    }

    /// <summary>A forward step of the wall clock neither expires an outcome nor makes the snapshot report it older than it is.</summary>
    [Test]
    public async Task WallStepDoesNotAgeOutcome()
    {
        var clock = new SteppedWallClock();
        var state = new GroupIdempotencyState(4, Retention, clock);
        Resolve(state);
        clock.Advance(AgeAtCapture);
        clock.StepWallClock(TimeSpan.FromHours(2));

        var retained = IsRetained(state);
        var outcomes = state.ExportResolved(out var capturedUtc);

        _ = await Assert.That(retained).IsTrue();
        _ = await Assert.That(capturedUtc - outcomes[0].ResolvedUtc!.Value).IsEqualTo(AgeAtCapture);
    }

    /// <summary>A restored outcome keeps aging on the monotonic clock, so it expires once the rest of its window has passed.</summary>
    [Test]
    public async Task RestoredOutcomeExpiresOnMonotonicClock()
    {
        var leader = new FakeTimeProvider(Epoch);
        var source = new GroupIdempotencyState(4, Retention, leader);
        Resolve(source);
        leader.Advance(AgeAtCapture);
        var outcomes = source.ExportResolved(out var capturedUtc);

        var follower = new SteppedWallClock();
        var target = new GroupIdempotencyState(4, Retention, follower);
        target.RestoreFromSnapshot(outcomes, capturedUtc);

        // A backward wall step must not stretch the window.
        follower.StepWallClock(-TimeSpan.FromHours(5));
        follower.Advance(Retention - AgeAtCapture);

        _ = await Assert.That(IsRetained(target)).IsFalse();
    }

    private static void Resolve(GroupIdempotencyState state)
    {
        _ = state.Reserve("cache", "op", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = state.TryResolve("cache", "op", [9], 1UL, 1UL);
    }

    private static FollowerLogOptions Options(TimeProvider clock) => new() { TimeProvider = clock, IdempotencyRetention = Retention };

    private static bool IsRetained(GroupIdempotencyState state) => state.Lookup("cache", "op", [1], out _) == GroupIdempotencyLookup.Found;

    /// <summary>A fake clock whose wall time can step while its monotonic timestamp moves forward only.</summary>
    [ThreadSafe]
    private sealed class SteppedWallClock : FakeTimeProvider
    {
        private long _wallOffsetTicks;

        internal SteppedWallClock()
            : base(Epoch)
        {
        }

        public override DateTimeOffset GetUtcNow() => base.GetUtcNow().AddTicks(Interlocked.Read(ref _wallOffsetTicks));

        /// <summary>Reads the unstepped time, since the base derives timestamps from the virtual wall time; the monotonic clock never moves back.</summary>
        /// <returns>The monotonic timestamp.</returns>
        public override long GetTimestamp() => base.GetUtcNow().UtcTicks;

        internal void StepWallClock(TimeSpan step) => _ = Interlocked.Add(ref _wallOffsetTicks, step.Ticks);
    }
}
