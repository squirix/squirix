using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>A grouped mutation in flight at graceful stop completes instead of reporting a commit-unknown outcome.</summary>
[Immutable]
public sealed class GroupCommitStopTests : IsolatedStorageTestBase
{
    private const int CommitOutcomeUnknownEventId = 1015;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static readonly byte[] Payload = [1, 2, 3];

    /// <summary>A grouped mutation in flight at graceful stop applies to memory and reports no commit-unknown outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopAppliesParkedMutation(CancellationToken cancellationToken)
    {
        var options = CreateOptions();
        using var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        using var writer = new FlushSegmentWriter();
        await using var journal = new JournalCoordinator(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true), writer, NullLoggerFactory.Instance);
        var log = new EventRecordingLogger();
        var applied = 0;
        var appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new DurableMutationExecutor(journal, log);
        var key = CacheKey.Default("k");
        var mutation = executor.ExecuteAsync(
            key,
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, TaskCompletionSource Appended), int>(
                (journal, key, appended),
                static async (s, ownership, ct) =>
                {
                    await s.Journal.AppendPutAsync(ownership, s.Key, Payload, ct).ConfigureAwait(false);
                    s.Appended.SetResult();
                },
                (_, _) =>
                {
                    _ = Interlocked.Increment(ref applied);
                    return ValueTask.FromResult(1);
                }),
            CancellationToken.None).AsTask();

        await appended.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        _ = await Assert.That(await mutation.WaitAsync(Bound, TimeProvider.System, cancellationToken)).IsEqualTo(1);
        _ = await Assert.That(Volatile.Read(ref applied)).IsEqualTo(1);
        _ = await Assert.That(log.Count(CommitOutcomeUnknownEventId)).IsEqualTo(0);
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
        long IJournalSegmentWriter.Length => 0;

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
        {
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
