using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A retry of an idempotent mutation stalled on its journal fsync joins the execution in flight instead of reporting an unknown outcome,
/// and a retry after shutdown faulted it past its stamped frame reports the unknown outcome instead of executing again.
/// </summary>
[Immutable]
public sealed class RpcIdempotencyJoinTests : IsolatedStorageTestBase
{
    private const int CommitUnknownEventId = 1015;

    private const string Fingerprint = "fp-1";

    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private static readonly TimeSpan JoinObservationWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeyW = CacheKey.Default("w").ToString();

    private readonly Meter _testMeter = new("test");

    /// <summary>A retry with the same id and fingerprint waits for the stalled original and returns its outcome without executing again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryJoinsInFlightMutation(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore());
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var retry = target.PutAsync(Fingerprint, cancellationToken);
        var retryCompletedDuringStall = await Task.WhenAny(retry, Task.Delay(JoinObservationWindow, TimeProvider.System, cancellationToken)) == retry;
        journal.Writer.Flush.Release();
        var originalResponse = await original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var joinedResponse = await retry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(retryCompletedDuringStall).IsFalse();
        _ = await Assert.That(originalResponse.Added).IsTrue();
        _ = await Assert.That(joinedResponse.Added).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>A caller that gives up after its mutation was stamped still gets the outcome recorded, so a later retry replays it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PostStampCancelRecordsOutcome(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var store = CreateStore();
        var target = new PutTarget(journal.Journal, store);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, caller.Token);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await caller.CancelAsync();
        var completedWhileStalled = await Task.WhenAny(original, Task.Delay(JoinObservationWindow, TimeProvider.System, cancellationToken)) == original;
        journal.Writer.Flush.Release();
        var response = await original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var replayed = store.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var recorded);
        var retried = await target.PutAsync(Fingerprint, cancellationToken);

        _ = await Assert.That(completedWhileStalled).IsFalse();
        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(recorded).IsNotNull();
        _ = await Assert.That(recorded!.Added).IsTrue();
        _ = await Assert.That(retried.Added).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>
    /// The client pattern under a stall: the original attempt and a joined retry both hit their own deadlines, a later retry joins again
    /// and gets the outcome once the stall clears.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JoinedRetryObservesOutcomeAfterStall(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore());
        using var originalAttempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var firstRetryAttempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, originalAttempt.Token);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await originalAttempt.CancelAsync();
        var firstRetry = target.PutAsync(Fingerprint, firstRetryAttempt.Token);
        var firstRetryCompletedDuringStall = await Task.WhenAny(firstRetry, Task.Delay(JoinObservationWindow, TimeProvider.System, cancellationToken)) == firstRetry;
        await firstRetryAttempt.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(firstRetry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var secondRetry = target.PutAsync(Fingerprint, cancellationToken);
        journal.Writer.Flush.Release();
        var outcome = await secondRetry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(firstRetryCompletedDuringStall).IsFalse();
        _ = await Assert.That(outcome.Added).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>A retry that reuses the in-flight operation id with another fingerprint is rejected at once instead of joining.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JoinRejectsMismatchedFingerprint(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore());
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var mismatch = await NodeAsyncAssert.ThrowsAsync<ServerOpIdMismatchException>(
            target.PutAsync("fp-2", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var retry = target.PutAsync(Fingerprint, cancellationToken);
        journal.Writer.Flush.Release();
        _ = await original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var joined = await retry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(mismatch.Message).IsEqualTo(ServerOpIdMismatchException.StableDetail);
        _ = await Assert.That(joined.Added).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>
    /// Disposal over a group commit write that reached the file and then hung faults the original while its stamped frame may already be
    /// durable: the intent survives, so a retry reports the unknown outcome instead of executing again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAckShutdownKeepsIntent(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, true, ShutdownBudget, NullLogger.Instance, cancellationToken);

        // The first write also writes the segment header: warm up so the armed stall catches the frame under test.
        await journal.Journal.AppendPutAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore());
        journal.Writer.AfterWrite.Arm();

        var original = target.PutAsync(Fingerprint, cancellationToken);
        string crashImage;
        RpcException retryError;
        try
        {
            await journal.Writer.AfterWrite.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            crashImage = journal.ReadStampedPuts(cancellationToken);
            retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        _ = await Assert.That(crashImage).IsEqualTo(StallableJournal.Describe([$"{KeyA}#{OperationId}", KeyW]));
        _ = await Assert.That(retryError.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>Disposal during the outcome durability wait faults the original after stamping, so a retry reports the unknown outcome.</summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OutcomeWaitShutdownKeepsIntent(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore());
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, cancellationToken);
        RpcException retryError;
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>
    /// Disposal during the outcome durability wait faults the first caller after its mutation frame was stamped: it gets the stable
    /// commit-unknown contract with the shutdown fault logged as the cause, and a retry stays unknown without executing again.
    /// </summary>
    /// <param name="groupCommit">Whether the stuck fsync belongs to a group commit batch instead of a plain checkpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FirstCallerOutcomeFaultIsUnknown(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, ShutdownBudget, NullLogger.Instance, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore(), log);
        journal.Writer.Flush.Arm();

        var original = target.PutAsync(Fingerprint, cancellationToken);
        RpcException originalError;
        RpcException retryError;
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(journal.DisposeStalledAsync().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            originalError = await NodeAsyncAssert.ThrowsAsync<RpcException>(original.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await journal.ReclaimLeakedAsync(StallTimeout);
        }

        var logged = log.Find(CommitUnknownEventId);

        _ = await Assert.That(originalError.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(originalError.Status.Detail)).IsTrue();
        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(logged?.Cause).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(1);
    }

    /// <summary>
    /// A journal failure before the mutation frame is stamped keeps the first caller's definite failure and releases the intent, so a
    /// retry executes again instead of reporting an unknown outcome.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FirstCallerPreStampStaysDefinite(bool groupCommit, CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var target = new PutTarget(journal.Journal, CreateStore(), log);
        var reason = new IOException("journal device lost");
        journal.Journal.FailJournalPipeline(reason);

        var originalError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var retryError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(originalError).IsTypeOf<InvalidOperationException>().Because(originalError.ToString());
        _ = await Assert.That(retryError).IsTypeOf<InvalidOperationException>().Because(retryError.ToString());
        _ = await Assert.That(log.Find(CommitUnknownEventId)).IsNull();
        _ = await Assert.That(target.Executions).IsEqualTo(2);
    }

    /// <summary>A started intent rebuilt from the journal has no execution to join, so a retry reports the unknown outcome at once.</summary>
    /// <param name="withFingerprint">Whether the restored intent carries a fingerprint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RestoredIntentWithoutTaskStaysUnknown(bool withFingerprint, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var store = CreateStore();
        store.RestoreStarted(OperationId, withFingerprint ? Fingerprint : null, DateTime.UtcNow);
        var target = new PutTarget(journal.Journal, store);

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(target.PutAsync(Fingerprint, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(target.Executions).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    /// <summary>One idempotent put of a single key through the coordinator, the durable executor and a real (stallable) journal.</summary>
    [ThreadSafe]
    private sealed class PutTarget
    {
        private readonly RpcMutationIdempotencyCoordinator _coordinator;
        private readonly DurableMutationExecutor _executor;
        private readonly StrongBox<int> _executions = new(0);
        private readonly IJournalCoordinator _journal;
        private readonly AppliedKeys _memory = new();

        internal PutTarget(IJournalCoordinator journal, RpcMutationIdempotencyStore store)
            : this(journal, store, NullLogger.Instance)
        {
        }

        internal PutTarget(IJournalCoordinator journal, RpcMutationIdempotencyStore store, ILogger log)
        {
            _journal = journal;
            _executor = new DurableMutationExecutor(journal) { Log = log };
            _coordinator = new RpcMutationIdempotencyCoordinator(store, journal) { Log = log };
        }

        /// <summary>Gets how many times the mutation handler ran.</summary>
        internal int Executions => Volatile.Read(ref _executions.Value);

        internal Task<TryAddAsyncResponse> PutAsync(string fingerprint, CancellationToken cancellationToken) =>
            _coordinator.ExecuteAsync(
                OperationId,
                fingerprint,
                this,
                static async (s, ct) =>
                {
                    _ = Interlocked.Increment(ref s._executions.Value);
                    var applied = await s._memory.PutAsync(s._executor, s._journal, "a", ct).ConfigureAwait(false);
                    return new TryAddAsyncResponse { Added = applied == 1 };
                },
                cancellationToken);
    }
}
