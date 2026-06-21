using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Regression tests for durable journal ordering: fsync before in-memory apply.</summary>
public sealed class DurableMutationExecutorDurabilityTests : UnitTestBase
{
    /// <summary>Ensures a failed in-memory apply after durable journal is not retried.</summary>
    [Fact]
    public async Task MemoryApplyFailureAfterJournalIsNotRetried()
    {
        using var dir = new TempDirectory("squirix-durable-mutation-no-retry");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalBackend = JournalBackend.JsonFramed,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalWriter.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        var executor = new DurableMutationExecutor(journal);
        var applyCalls = 0;

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            _ = await executor.ExecuteAsync(EvaluateAsync, AppendJournalAsync, ApplyMemoryAsync, DefaultCancellationToken).AsTask();
        });

        Assert.Equal("memory apply failed", error.Message);
        Assert.Equal(1, applyCalls);
        Assert.Equal(1, journal.AppendedOps);
        return;

        async ValueTask AppendJournalAsync(CancellationToken cancellationToken)
        {
            await journal.AppendPutAsync(CacheKey.Default("k"), await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("v", null, null, 1, null), null, cancellationToken);
        }

        static ValueTask<DurableMutationCondition<int>> EvaluateAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply());
        }

        ValueTask<int> ApplyMemoryAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            applyCalls++;
            throw new InvalidOperationException("memory apply failed");
        }
    }
}
