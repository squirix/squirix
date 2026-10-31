using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>One replicated write waiting for, or taken into, a commit batch of its group.</summary>
/// <remarks>
/// <para>
/// A request moves <see cref="Queued" /> to <see cref="Taken" />, <see cref="Leading" />, or <see cref="Canceled" /> exactly once, by a
/// compare-and-swap, so a caller that gives up and the leader that takes the request never both win: only a queued request can be canceled, and a taken
/// one is decided by the leader whatever its caller does. A request that finds no leader starts as <see cref="Leading" />; a queued one becomes
/// <see cref="Leading" /> when the previous leader hands the role over, and then takes the commit gate in its own call.
/// </para>
/// <para>
/// The leader writes <see cref="Outcome" /> or <see cref="Failure" /> before it completes the signal, so the caller reads them after the wait.
/// </para>
/// </remarks>
[ThreadSafe]
internal abstract class ReplicaCommitRequest
{
    private const int Queued = 0;
    private const int Taken = 1;
    private const int Leading = 2;
    private const int Canceled = 3;

    private readonly TaskCompletionSource<bool> _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _completed;
    private int _state;

    /// <summary>Initializes a new instance of the <see cref="ReplicaCommitRequest" /> class.</summary>
    /// <param name="scope">The cache scope of the write.</param>
    /// <param name="operationId">The client operation identifier of the write.</param>
    /// <param name="key">The cache key the write decides.</param>
    protected ReplicaCommitRequest(string scope, string operationId, string key)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(operationId);
        ArgumentNullException.ThrowIfNull(key);
        Scope = scope;
        OperationId = operationId;
        Key = key;
    }

    /// <summary>Gets the error the write failed with; <see langword="null" /> when it succeeded or is not decided yet.</summary>
    internal Exception? Failure { get; private set; }

    /// <summary>Gets a value indicating whether the leader decided this request.</summary>
    internal bool IsCompleted => Volatile.Read(ref _completed) != 0;

    /// <summary>Gets the cache key the write decides.</summary>
    internal string Key { get; }

    /// <summary>Gets the client operation identifier of the write.</summary>
    internal string OperationId { get; }

    /// <summary>Gets the committed outcome of the write.</summary>
    internal ReadOnlyMemory<byte> Outcome { get; private set; }

    /// <summary>Gets the cache scope of the write.</summary>
    internal string Scope { get; }

    /// <summary>Waits for the leadership of this queued request, or for the decision of the leader that took it.</summary>
    /// <param name="cancellationToken">Ends the wait while the request is still queued.</param>
    /// <returns><see langword="true" /> when the previous leader handed the leadership to this request; <see langword="false" /> when a leader decided it.</returns>
    /// <exception cref="OperationCanceledException">The token ended while the request was still queued, so no leader will take it.</exception>
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The request creates the source and completes it only from its leader: the promotion, the decision, or the failure.")]
    internal async Task<bool> AwaitLeadershipAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _signal.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Cancels only while the request is queued: a leader that took it, or handed it the leadership, decides it, and the caller waits for that.
            if (TryCancel())
                throw;
        }

        return await _signal.Task.ConfigureAwait(false);
    }

    /// <summary>Marks a request that found no leader as the leader.</summary>
    internal void BeginLeading() => Volatile.Write(ref _state, Leading);

    /// <summary>Cancels the request of a leader that gave up before it took the request into a batch.</summary>
    internal void CancelLeading() => Volatile.Write(ref _state, Canceled);

    /// <summary>Records the failure of the write and wakes its caller.</summary>
    /// <param name="failure">The error to rethrow to the caller.</param>
    internal void Fail(Exception failure)
    {
        Failure = failure;
        Complete();
    }

    /// <summary>Reads the decision of the write after its wait ended.</summary>
    /// <returns>The committed outcome payload.</returns>
    /// <exception cref="Exception">The failure the write was decided with, rethrown with its own stack trace kept.</exception>
    internal ReadOnlyMemory<byte> OutcomeOrThrow()
    {
        if (Failure is { } failure)
            ExceptionDispatchInfo.Capture(failure).Throw();

        return Outcome;
    }

    /// <summary>Hands a taken request back to the queue, so another batch decides it.</summary>
    /// <remarks>The request is still at the head of the queue; its caller may cancel it again.</remarks>
    internal void Release() => Volatile.Write(ref _state, Queued);

    /// <summary>Records the committed outcome of the write and wakes its caller.</summary>
    /// <param name="outcome">The outcome payload.</param>
    internal void Succeed(ReadOnlyMemory<byte> outcome)
    {
        Outcome = outcome;
        Complete();
    }

    /// <summary>Takes the leading request into the batch it leads.</summary>
    internal void TakeLeading() => Volatile.Write(ref _state, Taken);

    /// <summary>Asks the request to give up its place in the queue.</summary>
    /// <returns><see langword="true" /> when it was still queued and is now canceled; <see langword="false" /> once a leader holds it.</returns>
    internal bool TryCancel() => Interlocked.CompareExchange(ref _state, Canceled, Queued) == Queued;

    /// <summary>Makes the request the leader.</summary>
    /// <returns><see langword="true" /> when it was still queued; <see langword="false" /> when it was canceled.</returns>
    internal bool TryPromote()
    {
        if (Interlocked.CompareExchange(ref _state, Leading, Queued) != Queued)
            return false;

        _ = _signal.TrySetResult(true);
        return true;
    }

    /// <summary>Takes a queued request into a batch.</summary>
    /// <returns><see langword="true" /> when it was still queued; <see langword="false" /> when it was canceled.</returns>
    internal bool TryTake() => Interlocked.CompareExchange(ref _state, Taken, Queued) == Queued;

    /// <summary>Tells whether the request was canceled before a leader took it.</summary>
    /// <returns><see langword="true" /> when canceled.</returns>
    internal bool IsCanceled() => Volatile.Read(ref _state) == Canceled;

    /// <summary>Looks up the entry the owned group log retains for the identity of this write.</summary>
    /// <param name="log">The owned group log, or <see langword="null" /> when it is not open.</param>
    /// <param name="record">The retained record when the lookup finds the outcome.</param>
    /// <returns>The lookup; a miss when no log is open or no entry is retained.</returns>
    internal abstract GroupIdempotencyLookup LookupRetained(IFollowerLog? log, out GroupIdempotencyRecord record);

    /// <summary>Prepares the mutation of this write at a log index.</summary>
    /// <param name="factory">The running mutation factory.</param>
    /// <param name="index">The log index the mutation is appended at.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns>The prepared mutation.</returns>
    internal abstract ValueTask<PreparedReplicaMutation> PrepareAsync(ReplicaMutationFactory factory, ulong index, CancellationToken cancellationToken);

    private void Complete()
    {
        Volatile.Write(ref _completed, 1);
        _ = _signal.TrySetResult(false);
    }
}
