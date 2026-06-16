using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Verifies journal snapshot cut error paths release the mutation gate.</summary>
public sealed class JournalWriterSnapshotCutReleaseTests : UnitTestBase
{
    /// <summary>Verifies journal mutation path is usable after a snapshot cut build phase throws.</summary>
    [Fact]
    public async Task SnapshotCutFailureStillAllowsJournalAppend()
    {
        using var dir = new TempDirectory("squirix-snap-cut-fail");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(persistence);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("v", null, null, 1, null);
        await journal.AppendPutAsync(CacheKey.Default("before"), payload, null, DefaultCancellationToken);

        _ = await Assert.ThrowsAsync<IOException>(() => journal.ExecuteSnapshotCutAsync(
            0,
            static (_, _, _) => new ValueTask<int>(0),
            static (_, _, _, _) => ValueTask.FromException<Manifest.SnapshotRef>(new IOException("simulated snapshot failure")),
            DefaultCancellationToken).AsTask());

        await journal.AppendPutAsync(CacheKey.Default("after"), payload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.Equal(2, journal.AppendedOps);
    }

    /// <summary>Ensures a snapshot cut cannot record a journal sequence while a durable mutation is still pending memory apply.</summary>
    [Fact]
    public async Task SnapshotCutWaitsForPendingMemoryApply()
    {
        using var dir = new TempDirectory("squirix-snap-cut-pending-apply");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(persistence);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var snapshotStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        journal.BeginPendingMemoryApply();
        var snapshotTask = journal.ExecuteSnapshotCutAsync(
            snapshotStarted,
            static (started, _, _) =>
            {
                started.SetResult();
                return new ValueTask<int>(1);
            },
            static (_, _, barrier, _) => new ValueTask<int>(barrier),
            DefaultCancellationToken).AsTask();

        var first = await Task.WhenAny(snapshotStarted.Task, Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, DefaultCancellationToken));
        Assert.NotSame(snapshotStarted.Task, first);

        journal.CompletePendingMemoryApply();

        Assert.Equal(1, await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken));
        Assert.True(snapshotStarted.Task.IsCompletedSuccessfully);
    }

    /// <summary>Verifies durable memory applies can proceed while snapshot serialization runs outside the mutation gate.</summary>
    [Fact]
    public async Task SnapshotCutBuildPhaseDoesNotBlockMutationBarrier()
    {
        using var dir = new TempDirectory("squirix-snap-cut-build-unblocked");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(persistence);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
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
                async _ =>
                {
                    mutationEntered.SetResult();
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

    private static Task<TResult> AsSingleUseTaskAsync<TResult>(ValueTask<TResult> valueTask) => valueTask.AsTask();
}
