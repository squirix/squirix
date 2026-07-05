using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Regression tests for durable journal ordering: fsync before in-memory apply.</summary>
public sealed class DurableMutationExecutorDurabilityTests : ServerUnitTestBase
{
    /// <summary>Ensures a failed in-memory apply after durable journal is not retried.</summary>
    /// <exception cref="InvalidOperationException">Thrown by the simulated in-memory apply delegate.</exception>
    [Fact]
    public async Task MemoryApplyFailureAfterJournalIsNotRetried()
    {
        using var dir = new TempDirectory("squirix-durable-mutation-no-retry");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(options);
        var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        try
        {
            var executor = new DurableMutationExecutor(journal);
            var applyCalls = 0;

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(
                EvaluateAsync,
                AppendJournalAsync,
                ApplyMemoryAsync,
                DefaultCancellationToken).AsTask());

            Assert.Equal("memory apply failed", error.Message);
            Assert.Equal(1, applyCalls);
            Assert.Equal(1, journal.AppendedOps);
            return;

            static ValueTask<DurableMutationCondition<int>> EvaluateAsync(CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
            }

            ValueTask AppendJournalAsync(CancellationToken cancellationToken)
            {
                return journal.AppendPutAsync(
                    CacheKey.Default("k"),
                    JournalEntryPayloadKit.EncodePut("v"),
                    cancellationToken);
            }

            ValueTask<int> ApplyMemoryAsync(CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                applyCalls++;
                throw new InvalidOperationException("memory apply failed");
            }
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }
}
