using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
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

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Regression tests for durable journal ordering: fsync before in-memory apply.</summary>
[Immutable]
public sealed class DurableMutationExecutorDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>Ensures a failed in-memory apply after durable journal is not retried.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">Thrown by the simulated in-memory apply delegate.</exception>
    [Test]
    public async Task MemoryFailureAfterJournalNotRetried(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var journal = JournalCoordinatorFactory.Create(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true));

        try
        {
            var executor = new DurableMutationExecutor(journal);
            var applyState = new ApplyCounter();

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, int>(
                executor.ExecuteAsync(
                    null,
                    static _ => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
                    new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload, ApplyCounter Apply), int>(
                        (journal, CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), applyState),
                        static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                        static (s, ct) => s.Apply.ApplyAsync(ct)),
                    cancellationToken));

            _ = await Assert.That(error.Message).IsEqualTo("memory apply failed");
            _ = await Assert.That(applyState.Calls).IsEqualTo(1);
            _ = await Assert.That(journal.AppendedOps).IsEqualTo(1);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    /// <summary>Precondition Skip returns the skip result without appending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreconditionSkipSkipsJournalAppend(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var journal = JournalCoordinatorFactory.Create(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true));

        try
        {
            var executor = new DurableMutationExecutor(journal);
            var applyState = new ApplyCounter(false);

            var result = await executor.ExecuteAsync(
                CacheKey.Default("skip-key"),
                static _ => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Skip(99)),
                new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload, ApplyCounter Apply), int>(
                    (journal, CacheKey.Default("skip-key"), JournalEntryPayloadKit.EncodePut("v"), applyState),
                    static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                    static (s, ct) => s.Apply.ApplyAsync(ct)),
                cancellationToken);

            _ = await Assert.That(result).IsEqualTo(99);
            _ = await Assert.That(applyState.Calls).IsEqualTo(0);
            _ = await Assert.That(journal.AppendedOps).IsEqualTo(0);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    private sealed class ApplyCounter
    {
        private readonly bool _throwOnApply;

        internal ApplyCounter(bool throwOnApply = true)
        {
            _throwOnApply = throwOnApply;
        }

        internal int Calls { get; private set; }

        internal ValueTask<int> ApplyAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Calls++;
            return _throwOnApply ? ValueTask.FromException<int>(new InvalidOperationException("memory apply failed")) : ValueTask.FromResult(1);
        }
    }
}
