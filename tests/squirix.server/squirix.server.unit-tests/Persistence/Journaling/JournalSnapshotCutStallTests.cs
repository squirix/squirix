using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>A snapshot cut whose checkpoint fsync is stalled must not keep other mutations out of the barrier.</summary>
[Immutable]
public sealed class JournalSnapshotCutStallTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan GateProbeWindow = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeysBc = $"{CacheKey.Default("b")},{CacheKey.Default("c")}";

    /// <summary>While the snapshot cut's checkpoint flush is stalled, another mutation enters the barrier.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotCutDoesNotHoldGateAcrossFlush(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        journal.Writer.Flush.Arm();

        // An unflushed frame makes the cut's checkpoint issue a real fsync, which is what the stall blocks.
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var cut = journal.Journal.ExecuteSnapshotCutAsync(
            0,
            static (_, sequence, _) => ValueTask.FromResult(sequence),
            static (_, _, sequence, _) => ValueTask.FromResult(sequence),
            cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mutation = journal.Journal.ExecuteUnderSnapshotBarrierAsync(
            entered,
            static (signal, _) => ValueTask.FromResult(signal.TrySetResult()),
            cancellationToken).AsTask();
        var enteredDuringStall = await StallableJournal.CompletesWithinAsync(entered, GateProbeWindow, cancellationToken);
        journal.Writer.Flush.Release();
        _ = await cut;
        _ = await mutation;

        _ = await Assert.That(enteredDuringStall).IsTrue();
    }

    /// <summary>A frame appended while the cut waits for its checkpoint ack gets a sequence above the cut's watermark.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFrameSequenceAboveCutWatermark(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
        var cut = journal.Journal.ExecuteSnapshotCutAsync(
            0,
            static (_, sequence, _) => ValueTask.FromResult(sequence),
            static (_, _, sequence, _) => ValueTask.FromResult(sequence),
            cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sequence = new StrongBox<ulong>();
        var mutation = journal.Journal.ExecuteUnderSnapshotBarrierAsync(
            (journal.Journal, Appended: appended, Sequence: sequence),
            static async (s, ct) =>
            {
                await s.Journal.AppendPutAsync(CacheKey.Default("c"), JournalEntryPayloadKit.EncodePut("c"), ct);
                s.Sequence.Value = s.Journal.NextSequence;
                _ = s.Appended.TrySetResult();
            },
            cancellationToken).AsTask();
        var appendedDuringStall = await StallableJournal.CompletesWithinAsync(appended, GateProbeWindow, cancellationToken);
        journal.Writer.Flush.Release();
        var watermark = await cut;
        await mutation;

        _ = await Assert.That(appendedDuringStall).IsTrue();
        _ = await Assert.That(sequence.Value).IsGreaterThan(watermark);
    }

    /// <summary>A snapshot cut taken while its flush is stalled and followed by a removal restarts into the live state, not a resurrected key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledCutRecoveryMatchesLiveState(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);
        _ = await memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        _ = await memory.PutAsync(executor, journal.Journal, "b", cancellationToken);

        // An unflushed frame makes the cut's checkpoint issue a real fsync; re-putting an applied key keeps memory and the WAL equal.
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        journal.Writer.Flush.Arm();
        var cut = journal.Journal.ExecuteSnapshotCutAsync(
            memory,
            static (m, _, _) => ValueTask.FromResult(m.Snapshot),
            static (_, cutSequence, snapshot, _) => ValueTask.FromResult((cutSequence, snapshot)),
            cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var remove = memory.RemoveAsync(executor, journal.Journal, "a", cancellationToken);
        var put = memory.PutAsync(executor, journal.Journal, "c", cancellationToken);
        journal.Writer.Flush.Release();
        var (sequence, captured) = await cut;
        _ = await remove;
        _ = await put;
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);
        var restarted = journal.Recover(captured, sequence, cancellationToken);

        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeysBc);
        _ = await Assert.That(replayed).IsEqualTo(KeysBc);
        _ = await Assert.That(restarted).IsEqualTo(KeysBc);
    }
}
