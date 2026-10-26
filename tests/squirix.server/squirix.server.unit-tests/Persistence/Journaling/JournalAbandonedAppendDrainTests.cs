using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Abandoned ring items: when the journal thread can no longer complete admitted
/// appends, their uncancellable acks must fail and the queued-appends counter must return to
/// baseline instead of hanging and leaking.
/// </summary>
[Immutable]
public sealed class JournalAbandonedAppendDrainTests : IsolatedStorageTestBase
{
    private const int FillPayloadSize = 8_192;
    private const int LargePayloadSize = 16_000;
    private const int PendingAppends = 4;

    /// <summary>
    /// A failed segment roll fails the pipeline: durable appends parked behind the
    /// roll-deferred frame fault instead of hanging, and the counter returns to baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedRollFailsPendingDurableAppends(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options, NullLogger<Ledger>.Instance);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true), NullLoggerFactory.Instance, TimeProvider.System, out _);
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutDurablyUnderGateAsync(CacheKey.Default("seed"), payload, cancellationToken);
        var baseline = pipelined.QueuedAppendsCounter.Value;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        await FillSegmentOneForOverflowAsync(pipelined, FrameLength(overflowPayload, overflowKey), cancellationToken);

        // A disposed ledger refuses every queued roll, so the journal-triggered roll fails.
        ledger.Dispose();

        // The overflow append does not wait for durability, so the test continues while it parks
        // on the roll-deferred frame. The durable followers queue behind it in ring order and never
        // dequeue while it is parked, so the drain below faults all of them.
        await journal.AppendPutUnderGateAsync(overflowKey, overflowPayload, cancellationToken);
        var pending = StartDurableAppends(journal, payload, AppendDurableAsync, cancellationToken);

        await pipelined.WaitUntilAsync(static j => j.GetJournalThreadFailure() != null, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(journal.GetJournalThreadFailure()).IsNotNull();

        var all = Task.WhenAll(pending);
        await all.WaitUntilAsync(static t => t.IsCompleted, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(all.IsFaulted).IsTrue().Because("Append tasks completed instead of faulting.");
        _ = all.Exception;

        // Task.WhenAll faults when any constituent faults: assert every append faulted, not just one.
        for (var i = 0; i < pending.Count; i++)
        {
            _ = await Assert.That(pending[i].IsFaulted).IsTrue().Because("Append task completed instead of faulting.");
            _ = pending[i].Exception;
        }

        _ = await Assert.That(pipelined.QueuedAppendsCounter.Value).IsEqualTo(baseline);
    }

    /// <summary>
    /// Starts a durable append with only its admission under the gate, so every frame queues on the ring behind the parked one while its
    /// durability wait runs outside the gate. The roll may fail fast on the refused manifest while this thread is still queueing: the pipeline
    /// may already be dead, and the refusal faults the returned task like the async drain path does; the asserts only require every append
    /// to fault.
    /// </summary>
    /// <param name="journal">Journal to append to.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="payload">Encoded cache entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The durable append.</returns>
    private static Task AppendDurableAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, CancellationToken cancellationToken) =>
        journal.AppendAdmittedUnderGateAsync(
            (Key: key, Payload: payload),
            static (appender, s, ownership, ct) => appender.AppendPutAndAwaitCommitAsync(ownership, s.Key, s.Payload, ct),
            cancellationToken);

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
    };

    private static async Task FillSegmentOneForOverflowAsync(JournalCoordinator journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = new byte[FillPayloadSize];
        Array.Fill(fillPayload, Convert.ToByte('x'));
        var fillKey = CacheKey.Default("fill");
        var fillFrameLen = FrameLength(fillPayload, fillKey);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex == 1; i++)
        {
            if (journal.EventLoop.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes)
                break;

            if (journal.EventLoop.ActiveSegmentWrittenBytes + fillFrameLen > maxBytes)
                break;

            await journal.AppendPutUnderGateAsync(fillKey, fillPayload, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        _ = await Assert.That(journal.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(journal.EventLoop.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes).IsTrue();
    }

    private static int FrameLength(ReadOnlyMemory<byte> payload, CacheKey key)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.PrepareEncode(record).BodyLength);
    }

    private static List<Task> StartDurableAppends<TTarget>(
        TTarget target,
        byte[] payload,
        Func<TTarget, CacheKey, byte[], CancellationToken, Task> append,
        CancellationToken cancellationToken)
    {
        var pending = new List<Task>(PendingAppends);
        for (var i = 0; i < PendingAppends; i++)
            pending.Add(append(target, CacheKey.Default("pending-" + NodeInvariantIndexStrings.Format(i)), payload, cancellationToken));

        return pending;
    }
}
