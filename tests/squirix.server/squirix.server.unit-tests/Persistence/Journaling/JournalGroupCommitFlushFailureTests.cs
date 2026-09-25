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
/// A failed group commit flush must fail the journal pipeline: a later fsync can succeed after the kernel
/// dropped the dirty pages of the failed one, so it must never be retried and reported as durable.
/// </summary>
[Immutable]
public sealed class JournalGroupCommitFlushFailureTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private static readonly byte[] Payload = [1, 2, 3];

    /// <summary>A failed group commit flush faults the batch, latches the pipeline failure and is never retried.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedFlushFailsPipelineWithoutRetry(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options);
        using var writer = new FailOnceFsyncSegmentWriter();
        await using var journal = new JournalCoordinator(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            writer);
        var key = new CacheKey("ns", "k");

        await journal.AppendPutUnderGateAsync(key, Payload, cancellationToken);
        var first = await NodeAsyncAssert.ThrowsAsync<IOException>(journal.AwaitDurabilityCommitAsync(cancellationToken).AsTask().WaitAsync(Bound, TimeProvider.System, cancellationToken));
        _ = await Assert.That(first).IsSameReferenceAs(writer.Failure);

        // The waiter is faulted before the journal thread latches the pipeline failure; joining the thread waits for the latch.
        _ = await Assert.That(await journal.DurabilityPipeline.TryJoinJournalThreadAsync(Bound)).IsTrue();
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(CommitAgainAsync(journal, key, cancellationToken).WaitAsync(Bound, TimeProvider.System, cancellationToken));
        _ = await Assert.That(writer.FlushCount).IsEqualTo(1);
    }

    private static async Task CommitAgainAsync(JournalCoordinator journal, CacheKey key, CancellationToken cancellationToken)
    {
        await journal.AppendPutUnderGateAsync(key, Payload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 4,
        FlushInterval = 600_000,
        ManifestRetentionCount = 1,
        JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(20),
        JournalGroupCommitMaxBatch = 1,
    };

    private sealed class FailOnceFsyncSegmentWriter : IJournalSegmentWriter
    {
        private int _flushCount;

        long IJournalSegmentWriter.Length => 0;

        internal int FlushCount => Volatile.Read(ref _flushCount);

        internal IOException Failure { get; } = new("simulated fsync failure");

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
            if (Interlocked.Increment(ref _flushCount) == 1)
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
    }
}
