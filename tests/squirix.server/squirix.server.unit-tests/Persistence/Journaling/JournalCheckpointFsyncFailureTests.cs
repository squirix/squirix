using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// A durability checkpoint removes its ack from the registry before fsync, so the failure drain can
/// no longer reach it: a failed fsync must fault that ack directly instead of leaving its waiter hung.
/// </summary>
[Immutable]
public sealed class JournalCheckpointFsyncFailureTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private static readonly byte[] Payload = [1, 2, 3];

    /// <summary>A checkpoint waiter cancelled while its fsync is in flight observes the fsync failure instead of hanging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledCheckpointObservesFsyncFailure(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new FailingFsyncSegmentWriter(true);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer);
        await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), Payload, cancellationToken);

        using var waiterCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pending = journal.AwaitDurabilityCommitAsync(waiterCancellation.Token).AsTask();
        try
        {
            await writer.FsyncEntered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            await waiterCancellation.CancelAsync();
        }
        finally
        {
            writer.ReleaseFsync();
        }

        var thrown = await NodeAsyncAssert.ThrowsAsync<IOException>(pending.WaitAsync(Bound, TimeProvider.System, cancellationToken));
        _ = await Assert.That(thrown).IsSameReferenceAs(writer.Failure);
        _ = await Assert.That(await journal.DurabilityPipeline.TryJoinJournalThreadAsync(Bound)).IsTrue();
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();
    }

    /// <summary>A failed checkpoint fsync faults the waiter with the I/O error and fails the journal pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedCheckpointFsyncFaultsWaiter(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new FailingFsyncSegmentWriter(false);
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer);
        await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), Payload, cancellationToken);

        var thrown = await NodeAsyncAssert.ThrowsAsync<IOException>(journal.AwaitDurabilityCommitAsync(cancellationToken).AsTask().WaitAsync(Bound, TimeProvider.System, cancellationToken));

        _ = await Assert.That(thrown).IsSameReferenceAs(writer.Failure);
        _ = await Assert.That(await journal.DurabilityPipeline.TryJoinJournalThreadAsync(Bound)).IsTrue();
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();
    }

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 4,
        FlushInterval = 600_000,
        ManifestRetentionCount = 1,
    };

    private sealed class FailingFsyncSegmentWriter : IJournalSegmentWriter
    {
        private readonly ManualResetEventSlim _fsyncRelease;
        private int _disposed;

        internal FailingFsyncSegmentWriter(bool holdFsync)
        {
            _fsyncRelease = new ManualResetEventSlim(!holdFsync);
        }

        long IJournalSegmentWriter.Length => 0;

        internal IOException Failure { get; } = new("simulated fsync failure");

        internal TaskCompletionSource FsyncEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases the fsync gate.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                _fsyncRelease.Dispose();
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
            _ = FsyncEntered.TrySetResult();
            _fsyncRelease.Wait();
            throw Failure;
        }

        void IJournalSegmentWriter.OpenSegment(string path, bool append)
        {
        }

        void IJournalSegmentWriter.Truncate(long length)
        {
        }

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
        }

        internal void ReleaseFsync() => _fsyncRelease.Set();
    }
}
