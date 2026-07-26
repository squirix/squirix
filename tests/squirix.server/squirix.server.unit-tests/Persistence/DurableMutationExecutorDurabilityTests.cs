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
    public async Task PreconditionSkipReturnsResultWithoutJournalAppend()
    {
        using var dir = new TempDirectory("squirix-durable-mutation-skip");
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
            var applyState = new ApplyCounter { ThrowOnApply = false };

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

    /// <summary>Stateful overload Apply path appends then applies.</summary>
    [Fact]
    public async Task StatefulOverloadApplyAppendsThenApplies()
    {
        using var dir = new TempDirectory("squirix-durable-mutation-stateful-apply");
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
            var key = CacheKey.Default("stateful-apply");
            var payload = JournalEntryPayloadKit.EncodePut("v");

            var result = await executor.ExecuteAsync(
                key,
                (Journal: journal, Key: key, Payload: payload),
                static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
                static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                static (_, _) => ValueTask.FromResult(11),
                DefaultCancellationToken);

            Assert.Equal(11, result);
            Assert.Equal(1, journal.AppendedOps);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    /// <summary>Stateful overload Skip path returns without appending.</summary>
    [Fact]
    public async Task StatefulOverloadSkipReturnsWithoutAppend()
    {
        using var dir = new TempDirectory("squirix-durable-mutation-stateful-skip");
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
            var appendCalls = new CallCounter();
            var applyCalls = new CallCounter();

            var result = await executor.ExecuteAsync(
                CacheKey.Default("stateful-skip"),
                7,
                static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Skip(7)),
                appendCalls.AppendAsync,
                applyCalls.ApplyAsync,
                DefaultCancellationToken);

            Assert.Equal(7, result);
            Assert.Equal(0, appendCalls.Count);
            Assert.Equal(0, applyCalls.Count);
        }
        finally
        {
            await journal.DisposeAsync();
        }
    }

    private sealed class ApplyCounter
    {
        internal int Calls { get; private set; }

        internal bool ThrowOnApply { get; init; } = true;

        internal ValueTask<int> ApplyAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Calls++;
            if (ThrowOnApply)
                throw new InvalidOperationException("memory apply failed");

            return ValueTask.FromResult(1);
        }
    }

    private sealed class CallCounter
    {
        internal int Count { get; private set; }

        internal ValueTask AppendAsync(int state, CancellationToken cancellationToken)
        {
            _ = state;
            _ = cancellationToken;
            Count++;
            return ValueTask.CompletedTask;
        }

        internal ValueTask<int> ApplyAsync(int state, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Count++;
            return ValueTask.FromResult(state);
        }
    }
}
