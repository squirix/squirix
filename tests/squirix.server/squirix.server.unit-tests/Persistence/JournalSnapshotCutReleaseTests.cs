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
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Verifies journal snapshot cut error paths release the mutation gate.</summary>
[Immutable]
public sealed class JournalSnapshotCutReleaseTests : IsolatedStorageTestBase
{
    /// <summary>Verifies durable memory applies can proceed while snapshot serialization runs outside the mutation gate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutBuildDoesNotBlockMutationBarrier(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
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
                cancellationToken));

        await buildStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        var mutationTask = AsSingleUseTaskAsync(
            journal.ExecuteUnderSnapshotBarrierAsync(
                mutationEntered,
                static async (entered, _) =>
                {
                    entered.SetResult();
                    await Task.Yield();
                    return 42;
                },
                cancellationToken));

        var winner = await Task.WhenAny(mutationTask, Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, cancellationToken));
        _ = await Assert.That(winner).IsSameReferenceAs(mutationTask);
        _ = await Assert.That(await mutationTask).IsEqualTo(42);
        _ = await Assert.That(mutationEntered.Task.IsCompletedSuccessfully).IsTrue();

        releaseBuild.SetResult();
        _ = await Assert.That(await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken)).IsEqualTo(1);
    }

    /// <summary>Verifies a journal mutation path is usable after snapshot-cut-build phase throws.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutFailureStillAllowsJournalAppend(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("before"), payload, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<IOException, SnapshotRef>(
            journal.ExecuteSnapshotCutAsync(
                0,
                static (_, _, _) => new ValueTask<int>(0),
                static (_, _, _, _) => ValueTask.FromException<SnapshotRef>(new IOException("simulated snapshot failure")),
                cancellationToken));

        await journal.AppendPutUnderGateAsync(CacheKey.Default("after"), payload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(journal.AppendedOps).IsEqualTo(2);
    }

    /// <summary>Ensures a snapshot cut cannot record a journal sequence while a durable mutation is still pending memory apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotCutWaitsForPendingMemoryApply(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
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
            cancellationToken).AsTask();
        try
        {
            var first = await Task.WhenAny(snapshotStarted.Task, Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken));
            _ = await Assert.That(first).IsNotSameReferenceAs(snapshotStarted.Task);
        }
        finally
        {
            journal.InFlightApplyGate.Exit();
        }

        _ = await Assert.That(await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken)).IsEqualTo(1);
        _ = await Assert.That(snapshotStarted.Task.IsCompletedSuccessfully).IsTrue();
    }

    private static Task<TResult> AsSingleUseTaskAsync<TResult>(ValueTask<TResult> valueTask) => valueTask.AsTask();
}
