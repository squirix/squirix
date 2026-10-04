using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
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

    private static readonly TimeSpan UnreachedBatchDeadline = TimeSpan.FromHours(1);

    private static readonly byte[] Payload = [1, 2, 3];

    /// <summary>A grouped mutation parked in an open batch at graceful stop applies to memory and reports no commit-unknown outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopAppliesParkedMutation(CancellationToken cancellationToken)
    {
        await using var stallable = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var journal = stallable.Journal;
        var log = new EventRecordingLogger();
        var applied = 0;
        var executor = new DurableMutationExecutor(journal, log);
        var mutation = ExecutePutAsync(executor, journal, "k", () => Interlocked.Increment(ref applied));

        // The idle journal thread waits without a timeout until a waiter registers in the batch, so this pins the parked path.
        await journal.WaitUntilAsync(static j => j.GroupCommit!.GetJournalThreadWaitTimeoutMs() != Timeout.Infinite, Bound, cancellationToken);
        _ = await Assert.That(mutation.IsCompleted).IsFalse();

        await stallable.ShutdownAsync();

        _ = await Assert.That(await mutation.WaitAsync(Bound, TimeProvider.System, cancellationToken)).IsEqualTo(1);
        _ = await Assert.That(Volatile.Read(ref applied)).IsEqualTo(1);
        _ = await Assert.That(log.Count(CommitOutcomeUnknownEventId)).IsEqualTo(0);
    }

    /// <summary>A barrier wait without a frame of its own, parked at graceful stop, completes and a barrier issued after the stop succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BarrierWaitCompletesAtStop(CancellationToken cancellationToken)
    {
        await using var stallable = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var journal = stallable.Journal;
        var barrier = CommitAsync(journal);

        await stallable.ShutdownAsync();

        await barrier.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        await CommitAsync(journal).WaitAsync(Bound, TimeProvider.System, cancellationToken);
    }

    /// <summary>A grouped mutation started after the stop is refused before its append, so it never reaches the durability wait or reports commit-unknown.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutationAfterStopIsRefused(CancellationToken cancellationToken)
    {
        await using var stallable = await StallableJournal.CreateAsync(Dir, UnreachedBatchDeadline, 64, cancellationToken);
        var journal = stallable.Journal;
        var log = new EventRecordingLogger();
        var applied = 0;
        await stallable.ShutdownAsync();

        var late = ExecutePutAsync(new DurableMutationExecutor(journal, log), journal, "late", () => Interlocked.Increment(ref applied));
        var refused = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(late);

        _ = await Assert.That(refused is Squirix.Server.Errors.SquirixException).IsFalse();
        _ = await Assert.That(Volatile.Read(ref applied)).IsEqualTo(0);
        _ = await Assert.That(journal.AppendedOps).IsEqualTo(0);
        _ = await Assert.That(log.Count(CommitOutcomeUnknownEventId)).IsEqualTo(0);
    }

    /// <summary>
    /// A stop that times out over a slow final fsync faults the parked wait before the fsync completes; when that fsync later succeeds the
    /// group commit stays unsealed, so the parked and late waits both remain failed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutStopDoesNotSeal(CancellationToken cancellationToken)
    {
        await using var stallable = await StallableJournal.CreateAsync(Dir, true, TimeSpan.FromMilliseconds(250), NullLogger.Instance, cancellationToken);
        var journal = stallable.Journal;
        await journal.AppendPutUnderGateAsync(CacheKey.Default("k"), Payload, cancellationToken);
        var parked = CommitAsync(journal);
        stallable.Writer.Flush.Arm();

        // The marker's final fsync blocks; the stop gives up, faults the reachable waiters and reports the stuck thread.
        await journal.DisposeAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(parked.WaitAsync(Bound, TimeProvider.System, cancellationToken));

        // The slow fsync now succeeds and the thread exits cleanly, but the failure recorded by the stop wins.
        stallable.Writer.Flush.Release();
        _ = await Assert.That(await journal.DurabilityPipeline.TryJoinJournalThreadAsync(Bound)).IsTrue();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(CommitAsync(journal));
    }

    private static Task<int> ExecutePutAsync(DurableMutationExecutor executor, IJournalCoordinator journal, string key, Func<int> apply) =>
        executor.ExecuteAsync(
            CacheKey.Default(key),
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, Func<int> Apply), int>(
                (journal, CacheKey.Default(key), apply),
                static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, s.Key, Payload, ct),
                static (s, _) => ValueTask.FromResult(s.Apply())),
            CancellationToken.None).AsTask();

    private static Task CommitAsync(JournalCoordinator journal) => journal.AwaitDurabilityCommitAsync(CancellationToken.None).AsTask();
}
