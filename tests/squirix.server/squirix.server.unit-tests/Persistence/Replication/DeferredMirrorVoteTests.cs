using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A vote is answered after the first copy of its mirrored record is flushed; the mirror copy follows and every later publish and the dispose wait for it.</summary>
[Immutable]
public sealed class DeferredMirrorVoteTests : ServerUnitTestBase
{
    private const string GroupId = "deferred-mirror";

    private const int LeakedOnShutdownEventId = 4009;

    private const int MirrorFailedEventId = 4079;

    private const int MirrorFailedOnDisposeEventId = 4080;

    private const ulong VoteTerm = 4UL;

    /// <summary>The longest a call that must complete is waited for.</summary>
    private static readonly TimeSpan CompletionBudget = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>How long a call that must still be waiting is given to prove it does not complete.</summary>
    private static readonly TimeSpan StillWaiting = TimeSpan.FromMilliseconds(150);

    /// <summary>A dispose that outlasts its budget while waiting for the mirror copy leaks the handle loudly and faults the callers queued behind it with <see cref="ObjectDisposedException" />.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetExpiryFaultsQueuedCallers(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-budget");
        using var mirror = new MirrorHold();
        var logger = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var log = OpenLog(dir, mirror.Hooks, logger, clock);
        try
        {
            await log.OpenSeededAsync(cancellationToken);
            mirror.Arm();
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
            mirror.WaitHeld(cancellationToken);

            // The dispose takes the gate and arms its budget synchronously, so the caller issued next queues behind it.
            var dispose = log.DisposeAsync().AsTask();
            var queued = log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), cancellationToken);
            clock.Advance(ShutdownBudget);
            await dispose.WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(queued);
            _ = await Assert.That((logger.Count(LeakedOnShutdownEventId), logger.Find(LeakedOnShutdownEventId)?.Level)).IsEqualTo((1, LogLevel.Error));
        }
        finally
        {
            mirror.Release();
            IFollowerLogDurability durable = log;
            durable.Durability.Dispose();
            log.Metadata.Dispose();
            var metadataPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
            _ = SpinWait.SpinUntil(() => OpensForWrite(metadataPath), CompletionBudget);
        }
    }

    /// <summary>A vote canceled while the previous mirror copy is pending writes nothing and leaves that copy pending, so the next publish still waits for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledVoteKeepsPendingMirror(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-cancel");
        using var mirror = new MirrorHold();
        await using var log = OpenLog(dir, mirror.Hooks);
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);
        var publishesWhileHeld = mirror.Publishes;

        using var canceling = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var vote = log.RequestVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), canceling.Token);
        await Task.Delay(StillWaiting, TimeProvider.System, cancellationToken);
        await canceling.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(vote);

        var status = await log.GetStatusAsync(cancellationToken);
        var observe = log.ObserveTermAsync(VoteTerm + 2UL, cancellationToken);
        await Task.Delay(StillWaiting, TimeProvider.System, cancellationToken);
        var waitedWhileHeld = !observe.IsCompleted && mirror.Publishes == publishesWhileHeld;
        mirror.Release();
        var observed = await observe.WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
        _ = await Assert.That((waitedWhileHeld, observed, mirror.Publishes)).IsEqualTo((true, VoteTerm + 2UL, publishesWhileHeld + 2));
    }

    /// <summary>A damaged newest slot after the combined record leaves the term and the vote in the mirror copy, so another candidate is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DamagedNewestSlotKeepsTermAndVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-damage");
        await using (var log = OpenLog(dir, new CountingFollowerLogFaultHooks()))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken);
        }

        var file = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var newest = GroupMetadataStore.Read(file);
        await FollowerLogTestKit.CorruptByteAsync(file, (newest.Slot * GroupMetadataSlots.SlotSize) + 20, cancellationToken);

        await using var reopened = OpenLog(dir, new CountingFollowerLogFaultHooks());
        await reopened.OpenAsync(cancellationToken);

        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
        var other = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", VoteTerm, 0UL, 0UL), cancellationToken);
        _ = await Assert.That((other.Granted, other.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.AlreadyVoted));
    }

    /// <summary>A dispose waits for the pending mirror copy before the metadata handle closes, so both slots hold the record afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWaitsForMirrorCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-dispose");
        using var mirror = new MirrorHold();
        var log = OpenLog(dir, mirror.Hooks);
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);

        var dispose = log.DisposeAsync().AsTask();
        await Task.Delay(StillWaiting, TimeProvider.System, cancellationToken);
        var waitedWhileHeld = !dispose.IsCompleted;
        mirror.Release();
        await dispose.WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That(waitedWhileHeld).IsTrue();
        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        for (var slot = 0; slot < 2; slot++)
        {
            var alone = new byte[bytes.Length];
            bytes.CopyTo(alone, 0);
            Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);

            var read = GroupMetadataSlots.Select(alone);

            _ = await Assert.That((read.Metadata.CurrentTerm, read.Metadata.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
        }
    }

    /// <summary>A finished failed mirror copy fails readiness at the next gate entry, even one that publishes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMirrorCopyFailsNextEntry(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-entry");
        using var mirror = new MirrorHold { MirrorFailure = new IOException("simulated failure at the mirror copy.") };
        await using var log = OpenLog(dir, mirror.Hooks);
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);
        mirror.Release();

        // The failed copy finishes on a pool thread; each pre-vote is a gate entry that observes it once it has.
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(CompletionBudget);
        while (log.Readiness != FollowerLogReadiness.Failed)
        {
            _ = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), budget.Token);
            await Task.Yield();
        }

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
    }

    /// <summary>A failed mirror copy leaves the reported grant valid and durable, and a publish that was waiting for it fails with its error and fails readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMirrorCopyFailsNextPublish(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-fault");
        using var mirror = new MirrorHold { MirrorFailure = new IOException("simulated failure at the mirror copy.") };
        await using (var log = OpenLog(dir, mirror.Hooks))
        {
            await log.OpenSeededAsync(cancellationToken);
            mirror.Arm();
            var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
            _ = await Assert.That(granted.Granted).IsTrue();
            mirror.WaitHeld(cancellationToken);

            // The publish enters the gate while the copy is still held, so it is the publish that waits for it and observes its failure;
            // a failure that finished before the gate entry is covered by the next-entry test.
            var observe = log.ObserveTermAsync(VoteTerm + 1UL, cancellationToken);
            _ = await Assert.That(observe.IsCompleted).IsFalse();
            mirror.Release();

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(observe);

            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((log.Readiness, status.CurrentTerm, status.VotedFor)).IsEqualTo((FollowerLogReadiness.Failed, VoteTerm, "node-b"));
        }

        await using var reopened = OpenLog(dir, new CountingFollowerLogFaultHooks());
        await reopened.OpenAsync(cancellationToken);

        var recovered = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((reopened.Readiness, recovered.CurrentTerm, recovered.VotedFor)).IsEqualTo((FollowerLogReadiness.Ready, VoteTerm, "node-b"));
    }

    /// <summary>A mirror copy that fails while the dispose waits for it is logged once, and the dispose still completes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMirrorCopyIsLoggedOnDispose(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-dispose-fault");
        using var mirror = new MirrorHold { MirrorFailure = new IOException("simulated failure at the mirror copy.") };
        var logger = new EventRecordingLogger();
        var log = OpenLog(dir, mirror.Hooks, logger, new FakeTimeProvider());
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);

        var dispose = log.DisposeAsync().AsTask();
        mirror.Release();
        await dispose.WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That((logger.Count(MirrorFailedOnDisposeEventId), logger.Count(MirrorFailedEventId), logger.Count(LeakedOnShutdownEventId))).IsEqualTo((1, 0, 0));
    }

    /// <summary>After a failed mirror copy and a reopen, another candidate in the same term is refused because the first copy kept the vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMirrorCopyKeepsVoteAfterReopen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-revote");
        using var mirror = new MirrorHold { MirrorFailure = new IOException("simulated failure at the mirror copy.") };
        var logger = new EventRecordingLogger();
        await using (var log = OpenLog(dir, mirror.Hooks, logger, new FakeTimeProvider()))
        {
            await log.OpenSeededAsync(cancellationToken);
            mirror.Arm();
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
            mirror.WaitHeld(cancellationToken);
            mirror.Release();

            // The failed copy finishes on a pool thread; each pre-vote is a gate entry that observes it once it has.
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            budget.CancelAfter(CompletionBudget);
            while (log.Readiness != FollowerLogReadiness.Failed)
            {
                _ = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), budget.Token);
                await Task.Yield();
            }

            _ = await Assert.That(logger.Count(MirrorFailedEventId)).IsEqualTo(1);
        }

        await using var reopened = OpenLog(dir, new CountingFollowerLogFaultHooks());
        await reopened.OpenAsync(cancellationToken);

        var other = await reopened.RequestVoteAsync(new ElectionVoteRequest("node-c", VoteTerm, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((other.Granted, other.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.AlreadyVoted));
    }

    /// <summary>A grant at a higher term stores the term and the vote as one record in both slots, in two publishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermGrantStoresOneRecord(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-combined");
        var hooks = new CountingFollowerLogFaultHooks();
        await using (var log = OpenLog(dir, hooks))
        {
            await log.OpenSeededAsync(cancellationToken);
            var before = hooks.Snapshot();

            var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken);
            await log.DisposeAsync();

            _ = await Assert.That((granted.Granted, granted.CurrentTerm)).IsEqualTo((true, VoteTerm));
            _ = await Assert.That(hooks.Snapshot().Since(in before).MetaWrites).IsEqualTo(2L);
        }

        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        for (var slot = 0; slot < 2; slot++)
        {
            var alone = new byte[bytes.Length];
            bytes.CopyTo(alone, 0);
            Array.Clear(alone, (1 - slot) * GroupMetadataSlots.SlotSize, GroupMetadataSlots.SlotSize);

            var read = GroupMetadataSlots.Select(alone);

            _ = await Assert.That((read.Status, read.Metadata.CurrentTerm, read.Metadata.VotedFor)).IsEqualTo((GroupMetadataReadStatus.Valid, VoteTerm, "node-b"));
        }
    }

    /// <summary>A later metadata publish waits for the pending mirror copy, so the store is never written concurrently.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LaterPublishWaitsForMirrorCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-later");
        using var mirror = new MirrorHold();
        await using var log = OpenLog(dir, mirror.Hooks);
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);
        var publishesWhileHeld = mirror.Publishes;

        var observe = log.ObserveTermAsync(VoteTerm + 1UL, cancellationToken);
        await Task.Delay(StillWaiting, TimeProvider.System, cancellationToken);
        var waitedWhileHeld = !observe.IsCompleted && mirror.Publishes == publishesWhileHeld;
        mirror.Release();
        var observed = await observe.WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That((waitedWhileHeld, observed, mirror.Publishes)).IsEqualTo((true, VoteTerm + 1UL, publishesWhileHeld + 2));
    }

    /// <summary>A vote refused at a higher term stores the term step alone, with no vote, in two publishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedHigherTermVoteStoresTermStepOnly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-refused");
        var hooks = new CountingFollowerLogFaultHooks();
        await using (var log = OpenLog(dir, hooks))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
            var before = hooks.Snapshot();

            var refused = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken);
            await log.DisposeAsync();

            _ = await Assert.That((refused.Granted, refused.RefusalCode, refused.CurrentTerm)).IsEqualTo((false, FollowerLogRefusal.StaleLog, VoteTerm));
            _ = await Assert.That(hooks.Snapshot().Since(in before).MetaWrites).IsEqualTo(2L);
        }

        var read = GroupMetadataStore.Read(GroupStoragePaths.GetMetadataPath(dir, GroupId));

        _ = await Assert.That((read.Metadata.CurrentTerm, read.Metadata.VotedFor)).IsEqualTo((VoteTerm, string.Empty));
    }

    /// <summary>The vote is reported and current in memory while the mirror copy is still held, and status reads and pre-vote do not wait for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplyPrecedesMirrorCopy(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-deferred-mirror-reply");
        using var mirror = new MirrorHold();
        await using var log = OpenLog(dir, mirror.Hooks);
        await log.OpenSeededAsync(cancellationToken);
        mirror.Arm();

        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", VoteTerm, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        mirror.WaitHeld(cancellationToken);
        var finishedWhenReplied = mirror.Finished;
        var status = await log.GetStatusAsync(cancellationToken).AsTask().WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        var preVote = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That((granted.Granted, preVote.Granted)).IsEqualTo((true, true));
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
        _ = await Assert.That((finishedWhenReplied, mirror.Finished)).IsEqualTo((false, false));
        mirror.Release();
    }

    /// <summary>Opens a log whose shutdown clock never advances, so a dispose waits for the mirror copy however slow the machine is; the budget tests advance their own clock.</summary>
    /// <param name="dir">The persistence root.</param>
    /// <param name="hooks">The fault hooks.</param>
    /// <returns>The log, not yet opened.</returns>
    private static FollowerLog OpenLog(TempDirectory dir, IFollowerLogFaultHooks hooks) =>
        OpenLog(dir, hooks, NullLogger<FollowerLog>.Instance, new FakeTimeProvider());

    private static FollowerLog OpenLog(TempDirectory dir, IFollowerLogFaultHooks hooks, ILogger<FollowerLog> logger, TimeProvider shutdownClock) =>
        new(
            dir,
            GroupId,
            GroupComposition.Create(GroupId),
            logger,
            new FollowerLogOptions { ShutdownTimeProvider = shutdownClock, ShutdownBudget = ShutdownBudget, TimeProvider = TimeProvider.System, FaultHooks = hooks });

    private static bool OpensForWrite(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    /// <summary>Fault hooks that hold the second metadata publish after they are armed, and optionally fail it when released.</summary>
    private sealed class MirrorHold : IDisposable
    {
        private readonly ManualResetEventSlim _held = new(false);
        private readonly ManualResetEventSlim _release = new(false);
        private int _armedAt = -1;
        private int _finished;
        private int _publishes;

        internal MirrorHold()
        {
            var expectations = new IFollowerLogFaultHooksCreateExpectations();
            _ = expectations.Setups.OnFrameWritten();
            _ = expectations.Setups.OnFramesScheduling();
            _ = expectations.Setups.OnFlushed();
            _ = expectations.Setups.OnCommitAdvanced();
            _ = expectations.Setups.OnBeforeMemoryApply();
            _ = expectations.Setups.OnMetaPublishing().Callback(OnPublish);
            Hooks = expectations.Instance();
        }

        internal bool Finished => Volatile.Read(ref _finished) != 0;

        internal IFollowerLogFaultHooks Hooks { get; }

        internal Exception? MirrorFailure { get; init; }

        internal int Publishes => Volatile.Read(ref _publishes);

        public void Dispose()
        {
            _held.Dispose();
            _release.Dispose();
        }

        /// <summary>Holds the second publish from now on: the first copy of the next record passes, its mirror copy blocks.</summary>
        internal void Arm() => Volatile.Write(ref _armedAt, Publishes + 2);

        internal void Release() => _release.Set();

        internal void WaitHeld(CancellationToken cancellationToken)
        {
            if (!_held.Wait(CompletionBudget, cancellationToken))
                throw new TimeoutException("The mirror copy was never held.");
        }

        private void OnPublish()
        {
            if (Interlocked.Increment(ref _publishes) != Volatile.Read(ref _armedAt))
                return;

            _held.Set();
            _ = _release.Wait(CompletionBudget, CancellationToken.None);
            Volatile.Write(ref _finished, 1);
            if (MirrorFailure != null)
                throw MirrorFailure;
        }
    }
}
