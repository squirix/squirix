using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// A failed maintenance action (Begin processed, End never enqueued) fails the journal
/// pipeline loudly instead of drifting silently.
/// </summary>
[Immutable]
public sealed class JournalMaintenanceAbortTests : IsolatedStorageTestBase
{
    /// <summary>
    /// Cancelling the maintenance token inside the Begin-without-End window fails the
    /// pipeline the same way a throwing action does: restart is required.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledActionFailsPipelineLoudly(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("seed"), payload, cancellationToken);

        using var cts = new CancellationTokenSource();
        var gate = new TornLayoutAction(Dir, new InvalidOperationException("compaction canceled"));
        var pending = journal.ExecuteMaintenanceExclusiveAsync(gate.RunAsync, cts.Token);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        await cts.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(pipelined, static coordinator => coordinator.DurabilityPipeline.ThrowIfJournalThreadFailed());

        var (cancelSegmentCount, cancelTotalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(pipelined.EventLoop.JournalSegmentCount).IsEqualTo(cancelSegmentCount);
        _ = await Assert.That(pipelined.UsedBytes).IsEqualTo(cancelTotalBytes);

        var appendError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(journal.AppendPutAsync(CacheKey.Default("post-cancel"), payload, cancellationToken));
        _ = await Assert.That(appendError.InnerException).IsNotNull();
    }

    /// <summary>
    /// A maintenance failure during shutdown teardown skips the abort publish (teardown
    /// already fails waiters loudly) but still fails the pipeline with the original error.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownSkipsAbortStillFailsLoudly(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("seed"), payload, cancellationToken);

        var gate = new TornLayoutAction(Dir, new InvalidOperationException("compaction failed on shutdown"));
        var pending = journal.ExecuteMaintenanceExclusiveAsync(gate.RunAsync, cancellationToken);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        // Quiescing producers initiates shutdown while the maintenance action is still
        // in flight, so the abort branch observes the shutdown flag when the action throws.
        // It throws on timeout, so reaching the next line proves quiescence succeeded.
        await pipelined.DurabilityPipeline.QuiesceProducersAsync([], TimeSpan.FromSeconds(10));
        _ = gate.Release.TrySetResult();

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pending);
        _ = await Assert.That(thrown).IsSameReferenceAs(gate.Original);

        var pipelineError = NodeExceptionAssert.For<InvalidOperationException>()
                                               .Throws(pipelined, static coordinator => coordinator.DurabilityPipeline.ThrowIfJournalThreadFailed());
        _ = await Assert.That(pipelineError.InnerException).IsSameReferenceAs(gate.Original);

        // The abort was skipped: the torn on-disk layout is NOT resynced into memory.
        var (shutdownSegmentCount, shutdownTotalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(pipelined.EventLoop.JournalSegmentCount).IsNotEqualTo(shutdownSegmentCount);
        _ = await Assert.That(pipelined.UsedBytes).IsNotEqualTo(shutdownTotalBytes);
    }

    /// <summary>
    /// A throwing maintenance action surfaces to the caller, fails the pipeline loudly,
    /// resyncs counters from disk, and later appends fail instead of drifting.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowingActionFailsPipelineLoudly(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("seed"), payload, cancellationToken);

        var failure = new TornLayoutFailure(Dir, new InvalidOperationException("compaction failed"));
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(journal.ExecuteMaintenanceExclusiveAsync(failure.ThrowAsync, cancellationToken));
        _ = await Assert.That(thrown).IsSameReferenceAs(failure.Original);

        var pipelineError = NodeExceptionAssert.For<InvalidOperationException>()
                                               .Throws(pipelined, static coordinator => coordinator.DurabilityPipeline.ThrowIfJournalThreadFailed());
        _ = await Assert.That(pipelineError.InnerException).IsSameReferenceAs(failure.Original);

        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(pipelined.EventLoop.JournalSegmentCount).IsEqualTo(segmentCount);
        _ = await Assert.That(pipelined.UsedBytes).IsEqualTo(totalBytes);

        var appendError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(journal.AppendPutAsync(CacheKey.Default("post-failure"), payload, cancellationToken));
        _ = await Assert.That(appendError.InnerException).IsSameReferenceAs(failure.Original);
    }

    private static Task WriteTornSegmentAsync(string dir, CancellationToken cancellationToken)
    {
        // Proxy a torn compaction layout: a rewritten segment appears on disk while the
        // in-memory counters are untouched (a separate new file avoids sharing the active
        // segment handle with the journal thread). Only a resync can reconcile them.
        var rewritten = Path.Join(dir, $"{FilePrefixes.Journal}000002{FileExtensions.Journal}");
        return File.WriteAllBytesAsync(rewritten, new byte[2048], cancellationToken);
    }

    private sealed class TornLayoutAction
    {
        private readonly string _dir;

        internal TornLayoutAction(string dir, InvalidOperationException original)
        {
            _dir = dir;
            Original = original;
        }

        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal InvalidOperationException Original { get; }

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            // Entry implies MaintenanceBegin was already acked (the coordinator awaits
            // the Begin ack before invoking the action). Tear the on-disk layout first so
            // resync assertions stay meaningful instead of trivially true, then wait for
            // the test to either cancel the token or release the throw below.
            await WriteTornSegmentAsync(_dir, cancellationToken);
            _ = Entered.TrySetResult();
            await Release.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            throw Original;
        }
    }

    private sealed class TornLayoutFailure
    {
        private readonly string _dir;

        internal TornLayoutFailure(string dir, InvalidOperationException original)
        {
            _dir = dir;
            Original = original;
        }

        internal InvalidOperationException Original { get; }

        internal async ValueTask ThrowAsync(CancellationToken cancellationToken)
        {
            await WriteTornSegmentAsync(_dir, cancellationToken);
            throw Original;
        }
    }
}
