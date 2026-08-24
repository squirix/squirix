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
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Verifies journal snapshot cut error paths release the mutation gate.</summary>
[Immutable]
public sealed class JournalSnapshotCutReleaseTests : IsolatedStorageTestBase
{
    /// <summary>Verifies durable memory applies can proceed while snapshot serialization runs outside the mutation gate.</summary>
    [Fact]
    public async Task CutBuildDoesNotBlockMutationBarrier()
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var buildStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBuild = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mutationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var snapshotTask = AsSingleUseTaskAsync(
            journal.ExecuteSnapshotCutAsync(
                (BuildStarted: buildStarted, ReleaseBuild: releaseBuild),
                static (state, _, _) =>
                {
                    state.BuildStarted.SetResult();
                    return new ValueTask<int>(1);
                },
                static async (state, _, barrier, ct) =>
                {
                    await state.ReleaseBuild.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct).ConfigureAwait(false);
                    return barrier;
                },
                DefaultCancellationToken));

        await buildStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);

        var mutationTask = AsSingleUseTaskAsync(
            journal.ExecuteUnderSnapshotBarrierAsync(
                mutationEntered,
                static async (entered, _) =>
                {
                    entered.SetResult();
                    await Task.Yield();
                    return 42;
                },
                DefaultCancellationToken));

        var winner = await Task.WhenAny(mutationTask, Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, DefaultCancellationToken));
        Assert.Same(mutationTask, winner);
        Assert.Equal(42, await mutationTask);
        Assert.True(mutationEntered.Task.IsCompletedSuccessfully);

        releaseBuild.SetResult();
        Assert.Equal(1, await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken));
    }

    /// <summary>Verifies journal mutation path is usable after a snapshot cut build phase throws.</summary>
    [Fact]
    public async Task CutFailureStillAllowsJournalAppend()
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAsync(CacheKey.Default("before"), payload, DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<IOException, SnapshotRef>(
            journal.ExecuteSnapshotCutAsync(
                0,
                static (_, _, _) => new ValueTask<int>(0),
                static (_, _, _, _) => ValueTask.FromException<SnapshotRef>(new IOException("simulated snapshot failure")),
                DefaultCancellationToken));

        await journal.AppendPutAsync(CacheKey.Default("after"), payload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.Equal(2, journal.AppendedOps);
    }

    /// <summary>Ensures a snapshot cut cannot record a journal sequence while a durable mutation is still pending memory apply.</summary>
    [Fact]
    public async Task SnapshotCutWaitsForPendingMemoryApply()
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var snapshotStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        journal.InFlightApplyGate.Enter();
        var snapshotTask = journal.ExecuteSnapshotCutAsync(
            snapshotStarted,
            static (started, _, _) =>
            {
                started.SetResult();
                return new ValueTask<int>(1);
            },
            static (_, _, barrier, _) => new ValueTask<int>(barrier),
            DefaultCancellationToken).AsTask();
        try
        {
            var first = await Task.WhenAny(snapshotStarted.Task, Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, DefaultCancellationToken));
            Assert.NotSame(snapshotStarted.Task, first);
        }
        finally
        {
            journal.InFlightApplyGate.Exit();
        }

        Assert.Equal(1, await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken));
        Assert.True(snapshotStarted.Task.IsCompletedSuccessfully);
    }

    private static Task<TResult> AsSingleUseTaskAsync<TResult>(ValueTask<TResult> valueTask) => valueTask.AsTask();
}
