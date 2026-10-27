using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Owns the pending mirror copy of a deferred metadata publish: its wait, its failure and its drain at dispose.</summary>
/// <remarks>The caller holds the log gate; the pending mirror copy is read and cleared only under it.</remarks>
[Immutable]
internal sealed class FollowerLogMirror
{
    private readonly TimeSpan _budget;
    private readonly TimeProvider _clock;
    private readonly string _groupId;
    private readonly FollowerLogJournal _journal;
    private readonly ILogger _log;

    internal FollowerLogMirror(FollowerLogJournal journal, ILogger log, string groupId, TimeSpan budget, TimeProvider clock)
    {
        _journal = journal;
        _log = log;
        _groupId = groupId;
        _budget = budget;
        _clock = clock;
    }

    /// <summary>Takes the pending mirror copy when it has finished, and logs its failure when it says something about the storage.</summary>
    /// <returns><see langword="true" /> when the finished mirror copy failed in a way that fails the readiness of the log.</returns>
    /// <remarks>An unfinished mirror copy is left pending; the next publish or the dispose waits for it.</remarks>
    internal bool TakeReadinessFailure()
    {
        if (_journal.Metadata.PendingMirror is not { IsCompleted: true } pending)
            return false;

        _journal.Metadata.PendingMirror = null;
        if (pending.Exception is not { } failure || !FollowerLogAppend.FailsReadiness(failure.GetBaseException()))
            return false;

        ServerLog.FollowerLogMirrorFailed(_log, failure.GetBaseException(), _groupId);
        return true;
    }

    /// <summary>Takes the log gate within the shutdown budget, which bounds the gate wait and the mirror copy wait together, and waits under it for the pending mirror copy.</summary>
    /// <param name="gate">The log gate.</param>
    /// <param name="acks">The registry of the tracked durable work.</param>
    /// <returns>The holder of the gate, or <see langword="null" /> when the budget expired and the gate was disposed instead.</returns>
    /// <remarks>
    /// The mirror copy of a deferred publish runs after the gate is released, so it is waited for under the gate, which no new publish can pass
    /// meanwhile. Its own failure is not a dispose failure: the first copy it follows is already the durable record. On expiry the gate goes
    /// first: faulting the in-flight waiter lets its append release the gate, which would hand it to a queued caller that then completes with a
    /// refusal instead of <see cref="ObjectDisposedException" />. The held holder is released only after the gate is disposed, or the gate
    /// would hand itself to a queued caller.
    /// </remarks>
    internal async Task<AsyncLockHolder?> AcquireDrainedGateAsync(AsyncLock gate, FollowerLogAckRegistry acks)
    {
        var heldHolder = default(AsyncLockHolder);
        using var expiry = new CancellationTokenSource(_budget, _clock);
        try
        {
            var lockGuard = await gate.LockAsync(expiry.Token).ConfigureAwait(false);
            heldHolder = lockGuard;
            await DrainAsync(expiry.Token).ConfigureAwait(false);
            return lockGuard;
        }
        catch (OperationCanceledException)
        {
            gate.Dispose();
            heldHolder.Dispose();
            var faulted = acks.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));
            ServerLog.FollowerLogLeakedOnShutdownTimeout(_log, _groupId, _budget, faulted);
            return null;
        }
    }

    [SuppressMessage("Usage", "VSTHRD003", Justification = "The pending mirror is the ack of a work item this log scheduled; the work item or the shutdown drain completes it.")]
    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        if (_journal.Metadata.PendingMirror is not { } pending)
            return;

        var finished = await Task.WhenAny(pending).WaitAsync(cancellationToken).ConfigureAwait(false);

        _journal.Metadata.PendingMirror = null;
        if (finished.Exception is { } failure)
            ServerLog.FollowerLogMirrorFailedOnDispose(_log, failure.GetBaseException(), _groupId);
    }
}
