using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Durable mutations under a stalled journal fsync: memory must match the WAL, and one stall must not serialize the gate.</summary>
[Immutable]
public sealed class DurableMutationStallTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan GateProbeWindow = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeysAb = $"{KeyA},{CacheKey.Default("b")}";

    /// <summary>A caller that cancels its durability wait after the append still leaves the mutation applied, exactly as the WAL replays it.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Skip("Fails until #675")]
    public async Task CanceledDurabilityWaitStillAppliesMemory(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var memory = new AppliedKeys();

        _ = await CancelDuringStalledFlushAsync(journal, memory, cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(replayed).IsEqualTo(KeyA);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(replayed);
    }

    /// <summary>While one mutation's fsync is stalled, a mutation on another key appends and applies; its acknowledgement still waits for durability.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Skip("Fails until #675")]
    public async Task GateNotHeldAcrossFsync(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);
        journal.Writer.Flush.Arm();

        var first = memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var second = memory.PutAsync(executor, journal.Journal, "b", cancellationToken);
        var secondAppliedDuringStall = await memory.AppliedWithinAsync("b", GateProbeWindow, cancellationToken);
        var secondAckedDuringStall = second.IsCompleted;
        journal.Writer.Flush.Release();
        _ = await first;
        _ = await second;

        _ = await Assert.That(secondAppliedDuringStall).IsTrue();
        _ = await Assert.That(secondAckedDuringStall).IsFalse();
    }

    /// <summary>A snapshot cut covering a frame whose durability wait was canceled recovers the same state as a full WAL replay and as memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Skip("Fails until #675")]
    public async Task SnapshotAfterCanceledWaitMatchesReplay(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var memory = new AppliedKeys();

        var executor = await CancelDuringStalledFlushAsync(journal, memory, cancellationToken);

        // A cut stops one sequence short of the newest frame, so only a later frame makes it cover the canceled one.
        _ = await memory.PutAsync(executor, journal.Journal, "b", cancellationToken);
        var (sequence, captured) = await journal.Journal.ExecuteSnapshotCutAsync(
            memory,
            static (m, _, _) => ValueTask.FromResult(m.Snapshot),
            static (_, cutSequence, snapshot, _) => ValueTask.FromResult((cutSequence, snapshot)),
            cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);
        var restarted = journal.Recover(captured, sequence, cancellationToken);

        _ = await Assert.That(replayed).IsEqualTo(KeysAb);
        _ = await Assert.That(restarted).IsEqualTo(replayed);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(replayed);
    }

    private static async Task<DurableMutationExecutor> CancelDuringStalledFlushAsync(StallableJournal journal, AppliedKeys memory, CancellationToken cancellationToken)
    {
        var executor = new DurableMutationExecutor(journal.Journal);
        journal.Writer.Flush.Arm();
        using var caller = new CancellationTokenSource();

        var put = memory.PutAsync(executor, journal.Journal, "a", caller.Token);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await caller.CancelAsync();

        // The flush is still blocked: a wait that honored the canceled caller would complete here.
        var completedWhileStalled = await Task.WhenAny(put, Task.Delay(CancelObservationWindow, TimeProvider.System, cancellationToken)) == put;
        _ = await Assert.That(completedWhileStalled).IsFalse();

        journal.Writer.Flush.Release();
        _ = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        return executor;
    }
}
