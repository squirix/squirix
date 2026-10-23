using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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

    private const ulong VoteTerm = 4UL;

    /// <summary>How long a call that must still be waiting is given to prove it does not complete.</summary>
    private static readonly TimeSpan StillWaiting = TimeSpan.FromMilliseconds(150);

    /// <summary>The longest a call that must complete is waited for.</summary>
    private static readonly TimeSpan CompletionBudget = TimeSpan.FromSeconds(30);

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
        var status = await log.GetStatusAsync(cancellationToken).AsTask().WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);
        var preVote = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", VoteTerm + 1UL, 0UL, 0UL), cancellationToken).WaitAsync(CompletionBudget, TimeProvider.System, cancellationToken);

        _ = await Assert.That((granted.Granted, preVote.Granted)).IsEqualTo((true, true));
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((VoteTerm, "node-b"));
        mirror.Release();
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

    /// <summary>A failed mirror copy leaves the reported grant valid and durable, and fails readiness at the next publish.</summary>
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
            mirror.Release();

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.ObserveTermAsync(VoteTerm + 1UL, cancellationToken));

            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((log.Readiness, status.CurrentTerm, status.VotedFor)).IsEqualTo((FollowerLogReadiness.Failed, VoteTerm, "node-b"));
        }

        await using var reopened = OpenLog(dir, new CountingFollowerLogFaultHooks());
        await reopened.OpenAsync(cancellationToken);

        var recovered = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((reopened.Readiness, recovered.CurrentTerm, recovered.VotedFor)).IsEqualTo((FollowerLogReadiness.Ready, VoteTerm, "node-b"));
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

    private static FollowerLog OpenLog(TempDirectory dir, IFollowerLogFaultHooks hooks) =>
        new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks });

    /// <summary>Fault hooks that hold the second metadata publish after they are armed, and optionally fail it when released.</summary>
    private sealed class MirrorHold : IDisposable
    {
        private readonly ManualResetEventSlim _held = new(false);
        private readonly ManualResetEventSlim _release = new(false);
        private int _armedAt = -1;
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

        internal void WaitHeld(CancellationToken cancellationToken) => _ = _held.Wait(CompletionBudget, cancellationToken);

        private void OnPublish()
        {
            if (Interlocked.Increment(ref _publishes) != Volatile.Read(ref _armedAt))
                return;

            _held.Set();
            _ = _release.Wait(CompletionBudget, CancellationToken.None);
            if (MirrorFailure is { } failure)
                throw failure;
        }
    }
}
