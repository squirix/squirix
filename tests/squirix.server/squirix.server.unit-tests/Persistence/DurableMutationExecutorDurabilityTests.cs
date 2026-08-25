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
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Regression tests for durable journal ordering: fsync before in-memory apply.</summary>
[Immutable]
public sealed class DurableMutationExecutorDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>Ensures a failed in-memory apply after durable journal is not retried.</summary>
    /// <exception cref="InvalidOperationException">Thrown by the simulated in-memory apply delegate.</exception>
    [Fact]
    public async Task MemoryFailureAfterJournalNotRetried()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate());

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
                    DefaultCancellationToken));

            Assert.Equal("memory apply failed", error.Message);
            Assert.Equal(1, applyState.Calls);
            Assert.Equal(1, journal.AppendedOps);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    /// <summary>Precondition Skip returns the skip result without appending.</summary>
    [Fact]
    public async Task PreconditionSkipSkipsJournalAppend()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate());

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
                DefaultCancellationToken);

            Assert.Equal(99, result);
            Assert.Equal(0, applyState.Calls);
            Assert.Equal(0, journal.AppendedOps);
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
            if (_throwOnApply)
                throw new InvalidOperationException("memory apply failed");

            return ValueTask.FromResult(1);
        }
    }
}
