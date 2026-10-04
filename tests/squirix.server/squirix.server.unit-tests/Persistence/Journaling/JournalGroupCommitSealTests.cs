using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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
/// A graceful stop seals the group commit after the final flush of the shutdown marker: waiters whose frames that flush covered
/// succeed, while a failure recorded earlier or a failed final flush keeps the commit outcome unknown.
/// </summary>
[Immutable]
public sealed class JournalGroupCommitSealTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static readonly byte[] Payload = [1, 2, 3];

    /// <summary>A waiter parked in an open batch at graceful stop completes successfully and later waits succeed at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopCompletesParkedWaiter(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        using var writer = new FlushSegmentWriter(null);
        await using var journal = new JournalCoordinator(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true), writer, NullLoggerFactory.Instance);
        await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), Payload, cancellationToken);
        var parked = CommitAsync(journal);
        _ = await Assert.That(parked.IsCompleted).IsFalse();

        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        await parked.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        await CommitAsync(journal).WaitAsync(Bound, TimeProvider.System, cancellationToken);
    }

    /// <summary>A failure recorded before the stop keeps parked and later waits failing, and the seal does not override it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierFailureStaysUnknown(CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        var options = CreateOptions();
        var groupCommit = new JournalDurabilityGroupCommit(static () => { }, static () => { }, options);
        var parked = groupCommit.AwaitCommitAsync(CancellationToken.None).AsTask();
        var failure = new IOException("pipeline failed");

        _ = await Assert.That(groupCommit.CancelPending(failure)).IsEqualTo(0);
        _ = await Assert.That(groupCommit.SealAfterFinalFlush()).IsEqualTo(0);

        _ = await Assert.That(await NodeAsyncAssert.ThrowsAsync<IOException>(parked)).IsSameReferenceAs(failure);
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(groupCommit.AwaitCommitAsync(CancellationToken.None));
    }

    /// <summary>A failed final flush of the shutdown marker leaves the parked waiter faulted, never completed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedFinalFlushStaysUnknown(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        using var writer = new FlushSegmentWriter(new IOException("simulated fsync failure"));
        await using var journal = new JournalCoordinator(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true), writer, NullLoggerFactory.Instance);
        await journal.AppendPutUnderGateAsync(new CacheKey("ns", "k"), Payload, cancellationToken);
        var parked = CommitAsync(journal);

        _ = await StopAsync(journal);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(parked.WaitAsync(Bound, TimeProvider.System, cancellationToken));
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(journal, cancellationToken, static (j, token) => _ = j.AwaitDurabilityCommitAsync(token).AsTask());
    }

    private static Task CommitAsync(JournalCoordinator journal) => journal.AwaitDurabilityCommitAsync(CancellationToken.None).AsTask();

    private static async Task<Exception?> StopAsync(JournalCoordinator journal)
    {
#pragma warning disable CA1031 // The failed final flush may also surface from the stop; the caller only observes the waiter outcome.
        try
        {
            await journal.DisposeAsync();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
#pragma warning restore CA1031
    }

    private PersistenceOptions CreateOptions() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 4,
        ManifestRetentionCount = 1,
        JournalGroupCommitMaxWait = TimeSpan.FromHours(1),
        JournalGroupCommitMaxBatch = 64,
    };

    private sealed class FlushSegmentWriter : IJournalSegmentWriter
    {
        private readonly IOException? _flushFailure;

        internal FlushSegmentWriter(IOException? flushFailure)
        {
            _flushFailure = flushFailure;
        }

        long IJournalSegmentWriter.Length => 0;

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
            if (_flushFailure != null)
                throw _flushFailure;
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
