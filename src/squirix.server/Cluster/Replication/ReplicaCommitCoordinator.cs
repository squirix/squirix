using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owns ordered durable majority commit for one fixed RF greater than one.</summary>
[ThreadSafe]
internal sealed class ReplicaCommitCoordinator : IAsyncDisposable
{
    internal const string CommitOutcomeUnknownCode = "COMMIT_OUTCOME_UNKNOWN";

    private readonly ReplicaMutationGate _admission;
    private readonly SemaphoreSlim _commitGate = new(1, 1);
    private readonly IReplicaCommitFaultHooks _faultHooks;
    private readonly GroupIdempotencyState _idempotency;
    private readonly Dictionary<OperationKey, CommitOperation> _operations = [];
    private readonly Lock _ownedSync = new();
    private readonly List<Task> _ownedTasks = [];
    private readonly SortedDictionary<ulong, PreparedReplicaMutation> _pendingApply = [];
    private readonly IReplicaCommitPipeline _pipeline;
    private readonly ReplicaCommitQuorum _quorum;
    private readonly ReplicaLogIndexSequencer _sequencer;
    private readonly ReplicaLogTurn _turn;
    private bool _accepting = true;
    private ulong _commitIndex;
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="ReplicaCommitCoordinator" /> class.</summary>
    /// <param name="options">Fixed group configuration.</param>
    /// <param name="pipeline">Durable and memory pipeline.</param>
    /// <param name="faultHooks">Fault-injection hooks.</param>
    /// <param name="idempotency">Bounded durable group idempotency state.</param>
    /// <param name="eligibility">
    /// Shared participation authority, also handed to repair sessions. When provided, replicas excluded by it
    /// contribute neither acknowledgements nor write-quorum copies until a repair session marks them ready.
    /// Activation wiring (RF&gt;1) owns the shared instance; <see langword="null" /> preserves the pre-activation behavior.
    /// </param>
    internal ReplicaCommitCoordinator(
        ReplicaCommitCoordinatorOptions options,
        IReplicaCommitPipeline pipeline,
        IReplicaCommitFaultHooks faultHooks,
        GroupIdempotencyState idempotency,
        ReplicaEligibility? eligibility = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(faultHooks);
        ArgumentNullException.ThrowIfNull(idempotency);

        _pipeline = pipeline;
        _faultHooks = faultHooks;
        _idempotency = idempotency;
        _quorum = new ReplicaCommitQuorum(options.ReplicaCount, options.InitialCommitIndex, eligibility);
        _sequencer = new ReplicaLogIndexSequencer(options.InitialLogIndex);
        _turn = new ReplicaLogTurn(options.InitialLogIndex);
        _admission = new ReplicaMutationGate(options.MaxInFlight);
        _commitIndex = options.InitialCommitIndex;
    }

    /// <summary>Observes all owned post-appending work before releasing resources.</summary>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>Failures already delivered via <see cref="CommitAsync" /> are observed, not rethrown.</remarks>
    public ValueTask DisposeAsync()
    {
        lock (_ownedSync)
        {
            _accepting = false;
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    /// <summary>Returns the highest contiguous durable index recorded for one replica.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The replica match index.</returns>
    /// <remarks>Test seam observing <see cref="ReplicaCommitQuorum.TryRecord" /> progress; production paths never poll it.</remarks>
    internal ulong MatchIndexFor(int replicaIndex) => _quorum.MatchIndexFor(replicaIndex);

    /// <summary>Commits a prepared mutation or reports an ambiguous post-appended outcome.</summary>
    /// <param name="mutation">Fully prepared immutable mutation.</param>
    /// <param name="timeout">Absolute pipeline budget.</param>
    /// <param name="cancellationToken">Client cancellation token.</param>
    /// <returns>The exact prepared successful outcome payload.</returns>
    /// <remarks>
    /// The caller assigns log indexes: they must be dense and strictly increasing per group, assigned
    /// under external serialization. A duplicate index fails deterministically; a gap stalls later commits
    /// until the pipeline budget expires. The coordinator never allocates indexes itself.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The mutation is out of order or its post-appending outcome is ambiguous.</exception>
    /// <exception cref="ObjectDisposedException">The coordinator is draining or disposed.</exception>
    internal async ValueTask<ReadOnlyMemory<byte>> CommitAsync(PreparedReplicaMutation mutation, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        var key = new OperationKey(mutation.OperationScope, mutation.OperationId);
        CommitOperation operation;
        Task<Task<ReadOnlyMemory<byte>>>? pendingStarter = null;
        lock (_ownedSync)
        {
            ObjectDisposedException.ThrowIf(!_accepting, this);

            var lookup = _idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint, out var retained);
            if (lookup == GroupIdempotencyLookup.Found)
                return retained.OutcomePayload;
            if (lookup == GroupIdempotencyLookup.Mismatch)
                throw new InvalidOperationException("Operation identifier was reused with a different fingerprint.");
            if (lookup == GroupIdempotencyLookup.Unresolved)
            {
                if (!_operations.TryGetValue(key, out operation!))
                    throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: retained operation requires recovery resolution.");
            }
            else
            {
                (operation, pendingStarter) = ReserveOperationLocked(key, mutation, timeout, cancellationToken);
            }

            OwnCore(operation.Resolution);
        }

        // Start the reserved execution after leaving the lock: the synchronous prefix of
        // ExecuteReservedAsync (admission, turn, gates, hooks, pipeline) must never begin
        // under _ownedSync. RunSynchronously executes inline without the thread pool.
        pendingStarter?.RunSynchronously(TaskScheduler.Default);

        try
        {
            return await operation.Resolution.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception error) when (operation.Attempt.IsLocallyAppended)
        {
            throw new InvalidOperationException($"{CommitOutcomeUnknownCode}: operation '{mutation.OperationId}'.", error);
        }
    }

    private static async Task<FollowerCompletion> AwaitFollowerAsync(int replicaIndex, Task<ReplicaDurableAcknowledgement> followerTask)
    {
        // The raw follower task is observed without deadline cancellation: aborting the majority loop
        // is the outer WaitAsync's job, while late durable responses must still reach RecordAcknowledgement
        // through the background observe path instead of being converted into error completions.
        var singleton = new List<Task<ReplicaDurableAcknowledgement>>(1) { followerTask };
        _ = await Task.WhenAny(singleton).ConfigureAwait(false);
        if (followerTask.IsCompletedSuccessfully)
        {
#pragma warning disable VSTHRD003 // The task was started by the current replication operation and is already complete.
            var acknowledgement = await followerTask.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            return new FollowerCompletion(replicaIndex, acknowledgement, null);
        }

        if (followerTask.IsCanceled)
            return new FollowerCompletion(replicaIndex, null, new OperationCanceledException());

        var aggregate = followerTask.Exception;
        return new FollowerCompletion(replicaIndex, null, aggregate?.InnerException ?? aggregate);
    }

    /// <summary>Reserves idempotency and registers the commit operation.</summary>
    /// <param name="key">Operation identity key.</param>
    /// <param name="mutation">Prepared mutation to execute.</param>
    /// <param name="timeout">Reservation timeout budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The registered operation and its starter task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when idempotency capacity is exhausted or the operation identifier is reused with a different fingerprint.</exception>
    /// <remarks>Must be called under <see cref="_ownedSync" />.</remarks>
    private (CommitOperation Operation, Task<Task<ReadOnlyMemory<byte>>> Starter) ReserveOperationLocked(
        OperationKey key,
        PreparedReplicaMutation mutation,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var recordKind = string.Equals(mutation.OperationScope, ReplicaExpirationOperationId.OperationScope, StringComparison.Ordinal) ? GroupRecordKind.Expiration
            : GroupRecordKind.UserMutation;
        var reserved = _idempotency.Reserve(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint, recordKind, mutation.LogIndex, mutation.Term);

        if (reserved == GroupIdempotencyReserveResult.CapacityExceeded)
            throw new InvalidOperationException("Group idempotency capacity is exhausted.");
        if (reserved == GroupIdempotencyReserveResult.FingerprintMismatch)
            throw new InvalidOperationException("Operation identifier was reused with a different fingerprint.");

        var attempt = new CommitAttempt();
        var starter = new Task<Task<ReadOnlyMemory<byte>>>(() => ExecuteReservedAsync(key, mutation, timeout, attempt, cancellationToken));
        var operation = new CommitOperation(attempt, starter.Unwrap());
        _operations[key] = operation;
        return (operation, starter);
    }

    private async Task CollectMajorityAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        var followerCount = _quorum.ReplicaCount - 1;
        var pending = new List<Task<FollowerCompletion>>(followerCount);
        var pendingReplicaIndexes = new HashSet<int>();
        var followerTasks = new HashSet<Task<ReplicaDurableAcknowledgement>>(ReferenceEqualityComparer.Instance);
        try
        {
            for (var replicaIndex = 1; replicaIndex < _quorum.ReplicaCount; replicaIndex++)
            {
                var followerTask = _pipeline.AppendFollowerAsync(replicaIndex, mutation, cancellationToken).AsTask();
                if (!followerTasks.Add(followerTask))
                {
                    _pipeline.RecordLaggingReplica(replicaIndex, mutation.LogIndex);
                    continue;
                }

                pending.Add(AwaitFollowerAsync(replicaIndex, followerTask));
                _ = pendingReplicaIndexes.Add(replicaIndex);
            }

            await _faultHooks.OnStageAsync(ReplicaCommitStage.FollowerFanOutStarted, mutation, cancellationToken).ConfigureAwait(false);
            while (_quorum.FindCommitIndex(_commitIndex, mutation.LogIndex) < mutation.LogIndex && pending.Count > 0)
                await RecordNextAcknowledgementAsync(pending, pendingReplicaIndexes, mutation, cancellationToken).ConfigureAwait(false);

            if (_quorum.FindCommitIndex(_commitIndex, mutation.LogIndex) < mutation.LogIndex)
                throw new InvalidOperationException("A durable majority was not reached before the deadline.");

            foreach (var replicaIndex in pendingReplicaIndexes)
                _pipeline.RecordLaggingReplica(replicaIndex, mutation.LogIndex);
        }
        finally
        {
            if (pending.Count > 0)
                Own(ObserveRemainingFollowersAsync(pending, mutation));
        }
    }

    private async Task DisposeCoreAsync()
    {
        try
        {
            // Bounded drain so disposal completes even when a follower task never finishes
            // (e.g. a pipeline ignoring cancellation). Remaining tasks keep exception observation.
            while (true)
            {
                List<Task> tasks;
                lock (_ownedSync)
                {
                    if (_ownedTasks.Count == 0)
                        break;

                    tasks = [.. _ownedTasks];
                    _ownedTasks.Clear();
                }

                try
                {
                    while (tasks.Count > 0)
                    {
                        var completed = await Task.WhenAny(tasks).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
                        _ = tasks.Remove(completed);
                        _ = completed.Exception;
                    }
                }
                catch (TimeoutException)
                {
                    foreach (var remaining in CollectionsMarshal.AsSpan(tasks))
                        _ = remaining.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                    break;
                }
            }
        }
        finally
        {
            _sequencer.Dispose();
            _admission.Dispose();
            _commitGate.Dispose();
        }
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteAsync(PreparedReplicaMutation mutation, TimeSpan timeout, CommitAttempt attempt, CancellationToken callerCancellation)
    {
        using var budgetCancellation = new CancellationTokenSource(timeout);
        using var preAppendCancellation = CancellationTokenSource.CreateLinkedTokenSource(callerCancellation, budgetCancellation.Token);
        using var lease = await _admission.EnterAsync(mutation.OperationId.GetHashCode(StringComparison.Ordinal), preAppendCancellation.Token).ConfigureAwait(false);
        await _turn.WaitAsync(mutation.LogIndex, preAppendCancellation.Token).ConfigureAwait(false);
        await _commitGate.WaitAsync(preAppendCancellation.Token).ConfigureAwait(false);
        try
        {
            return await ExecuteOrderedAsync(mutation, attempt, preAppendCancellation.Token, budgetCancellation.Token).ConfigureAwait(false);
        }
        finally
        {
            _ = _commitGate.Release();
        }
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteOrderedAsync(
        PreparedReplicaMutation mutation,
        CommitAttempt attempt,
        CancellationToken preAppendCancellation,
        CancellationToken resolutionCancellation)
    {
        using var reservation = await _sequencer.ReserveAsync(preAppendCancellation).ConfigureAwait(false);
        if (mutation.LogIndex != reservation.Index)
            throw new InvalidOperationException("Prepared mutation does not match the reserved group log index.");

        await _faultHooks.OnStageAsync(ReplicaCommitStage.Prepared, mutation, preAppendCancellation).ConfigureAwait(false);
        await _pipeline.AppendLocalAsync(mutation, preAppendCancellation).ConfigureAwait(false);
        attempt.MarkLocallyAppended();
        reservation.MarkAppended();

        // Every locally appended mutation is retained until a commit covers it, so a later commit whose
        // index jumps over an ambiguous predecessor can still apply the whole range in order. Entries are
        // accessed only from _commitGate-serialized ordered bodies, which also keeps apply order exact.
        _pendingApply[mutation.LogIndex] = mutation;
        _turn.Advance(mutation.LogIndex);

        await _faultHooks.OnStageAsync(ReplicaCommitStage.LocalAppendDurable, mutation, resolutionCancellation).ConfigureAwait(false);
        var leader = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        _ = _quorum.TryRecord(0, in leader, mutation);
        await CollectMajorityAsync(mutation, resolutionCancellation).ConfigureAwait(false);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.MajorityReached, mutation, resolutionCancellation).ConfigureAwait(false);
        var previousCommitIndex = _commitIndex;
        var commitIndex = _quorum.FindCommitIndex(previousCommitIndex, mutation.LogIndex);
        await _pipeline.AdvanceCommitIndexAsync(commitIndex, resolutionCancellation).ConfigureAwait(false);
        _commitIndex = commitIndex;
        await _faultHooks.OnStageAsync(ReplicaCommitStage.CommitIndexDurable, mutation, resolutionCancellation).ConfigureAwait(false);
        await ApplyPendingRangeAsync(commitIndex, resolutionCancellation).ConfigureAwait(false);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.MemoryApplied, mutation, resolutionCancellation).ConfigureAwait(false);
        await _faultHooks.OnStageAsync(ReplicaCommitStage.ResponseReady, mutation, resolutionCancellation).ConfigureAwait(false);
        return mutation.OutcomePayload;
    }

    private async Task ApplyPendingRangeAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        // Apply every retained entry at or below the new commit index in order, including entries
        // left behind when a prior ApplyMemoryAsync failure or cancellation interrupted the loop
        // (those would otherwise be skipped by a range starting right after the previous commit index).
        List<ulong>? due = null;
        foreach (var retained in _pendingApply.Keys)
        {
            if (retained > commitIndex)
                break;

            due ??= [];
            due.Add(retained);
        }

        if (due == null)
            return;

        foreach (var index in due)
        {
            if (!_pendingApply.TryGetValue(index, out var pending))
                continue;

            await _pipeline.ApplyMemoryAsync(pending, cancellationToken).ConfigureAwait(false);
            _ = _pendingApply.Remove(index);
        }
    }

    private async Task<ReadOnlyMemory<byte>> ExecuteReservedAsync(
        OperationKey key,
        PreparedReplicaMutation mutation,
        TimeSpan timeout,
        CommitAttempt attempt,
        CancellationToken cancellationToken)
    {
        try
        {
            var outcome = await ExecuteAsync(mutation, timeout, attempt, cancellationToken).ConfigureAwait(false);
            _ = _idempotency.TryResolve(mutation.OperationScope, mutation.OperationId, outcome, mutation.LogIndex, mutation.Term);
            lock (_ownedSync)
                _ = _operations.Remove(key);
            return outcome;
        }
        catch (Exception) when (!attempt.IsLocallyAppended)
        {
            _ = _idempotency.TryReleaseUnresolved(mutation.OperationScope, mutation.OperationId, mutation.LogIndex, mutation.Term);
            lock (_ownedSync)
                _ = _operations.Remove(key);
            throw;
        }

        // Post-append failures intentionally keep both pins: the unresolved idempotency record routes
        // same-identity retries to this ambiguous outcome instead of re-executing (which could apply
        // twice if the original did commit), and the faulted _operations entry lets those retries
        // observe it. Releasing either pin early is unsafe; reclamation happens only via journal
        // truncation (GroupIdempotencyState.ReleaseFromIndex). Size capacity for ambiguous-commit
        // bursts and watch GroupIdempotencyState.UnresolvedCount.
    }

    private async Task ObserveRemainingFollowersAsync(List<Task<FollowerCompletion>> pending, PreparedReplicaMutation mutation)
    {
        while (pending.Count > 0)
        {
            Task<FollowerCompletion> completed;
            try
            {
                completed = await Task.WhenAny(pending).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                foreach (var remaining in CollectionsMarshal.AsSpan(pending))
                    _ = remaining.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                break;
            }

            _ = pending.Remove(completed);
            var follower = await completed.ConfigureAwait(false);
            RecordAcknowledgement(in follower, mutation);
        }
    }

    private void Own(Task task)
    {
        lock (_ownedSync)
            OwnCore(task);
    }

    private void OwnCore(Task task)
    {
        for (var i = _ownedTasks.Count - 1; i >= 0; i--)
        {
            if (!_ownedTasks[i].IsCompleted)
                continue;

            _ = _ownedTasks[i].Exception;
            _ownedTasks.RemoveAt(i);
        }

        _ownedTasks.Add(task);
    }

    private void RecordAcknowledgement(in FollowerCompletion follower, PreparedReplicaMutation mutation)
    {
        if (follower.Acknowledgement is { } acknowledgement && _quorum.TryRecord(follower.ReplicaIndex, in acknowledgement, mutation))
            return;

        _pipeline.RecordLaggingReplica(follower.ReplicaIndex, mutation.LogIndex);
    }

    private async Task RecordNextAcknowledgementAsync(
        List<Task<FollowerCompletion>> pending,
        HashSet<int> pendingReplicaIndexes,
        PreparedReplicaMutation mutation,
        CancellationToken cancellationToken)
    {
        var completed = await Task.WhenAny(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        _ = pending.Remove(completed);
        var follower = await completed.ConfigureAwait(false);
        _ = pendingReplicaIndexes.Remove(follower.ReplicaIndex);
        RecordAcknowledgement(in follower, mutation);
    }

    [Immutable]
    private readonly record struct FollowerCompletion(int ReplicaIndex, ReplicaDurableAcknowledgement? Acknowledgement, Exception? Error);

    [Immutable]
    private readonly record struct OperationKey(string Scope, string OperationId);

    [Immutable]
    private sealed record CommitOperation(CommitAttempt Attempt, Task<ReadOnlyMemory<byte>> Resolution);

    [ThreadSafe]
    private sealed class CommitAttempt
    {
        private int _locallyAppended;

        internal bool IsLocallyAppended => Volatile.Read(ref _locallyAppended) != 0;

        internal void MarkLocallyAppended() => Volatile.Write(ref _locallyAppended, 1);
    }

    /// <summary>Orders prepared mutations by their preassigned group log index.</summary>
    [ThreadSafe]
    private sealed class ReplicaLogTurn
    {
        private readonly Lock _sync = new();
        private ulong _nextLogIndex;
        private TaskCompletionSource<bool> _turnAdvanced = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ReplicaLogTurn(ulong lastLogIndex)
        {
            if (lastLogIndex == ulong.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(lastLogIndex), "Replica log index is exhausted.");

            _nextLogIndex = lastLogIndex + 1;
        }

        internal void Advance(ulong logIndex)
        {
            TaskCompletionSource<bool> completed;
            lock (_sync)
            {
                if (logIndex != _nextLogIndex)
                    throw new InvalidOperationException("The durable append completed outside the expected group log order.");

                _nextLogIndex++;
                completed = _turnAdvanced;
                _turnAdvanced = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = completed.TrySetResult(true);
        }

        internal async ValueTask WaitAsync(ulong logIndex, CancellationToken cancellationToken)
        {
            while (true)
            {
                Task turnAdvanced;
                lock (_sync)
                {
                    if (logIndex < _nextLogIndex)
                        throw new InvalidOperationException("Prepared mutation is behind the next group log index.");
                    if (logIndex == _nextLogIndex)
                        return;

                    turnAdvanced = _turnAdvanced.Task;
                }

                await turnAdvanced.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
