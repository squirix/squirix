using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// Regression tests for durable journal ordering: fsync before in-memory apply.
/// </summary>
public sealed class DurableMutationExecutorDurabilityTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures a failed in-memory apply after durable journal is not retried.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task MemoryApplyFailureAfterJournalIsNotRetried()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-durable-mutation-no-retry");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 5,
                ManifestRetentionCount = 1,
            };

            var manifestStore = new ManifestStore(options);
            await using var journal = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var executor = new DurableMutationExecutor(journal);
            var applyCalls = 0;

            static ValueTask<DurableMutationCondition<int>> EvaluateAsync(CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
            }

            async ValueTask AppendJournalAsync(CancellationToken cancellationToken)
            {
                await journal.AppendPutAsync(CacheKey.Default("k"), DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null), null, cancellationToken)
                             .ConfigureAwait(false);
            }

            ValueTask<int> ApplyMemoryAsync(CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                applyCalls++;
                throw new InvalidOperationException("memory apply failed");
            }

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(
                EvaluateAsync,
                AppendJournalAsync,
                ApplyMemoryAsync,
                DefaultCancellationToken).AsTask());

            Assert.Equal("memory apply failed", error.Message);
            Assert.Equal(1, applyCalls);
            Assert.Equal(1, journal.AppendedOps);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
