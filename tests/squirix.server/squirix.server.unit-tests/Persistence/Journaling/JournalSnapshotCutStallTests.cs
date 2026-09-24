using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
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

    /// <summary>While the snapshot cut's checkpoint flush is stalled, another mutation enters the barrier.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Skip("Fails until #678")]
    public async Task SnapshotCutDoesNotHoldGateAcrossFlush(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        journal.Writer.Flush.Arm();

        // An unflushed frame makes the cut's checkpoint issue a real fsync, which is what the stall blocks.
        await journal.Journal.AppendPutAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
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
}
