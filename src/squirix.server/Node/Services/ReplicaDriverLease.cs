using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Keeps the passes over one group applier apart from a committer that leads the group by election.</summary>
/// <remarks>
/// The apply loop and the follower log maintenance enter a pass without waiting and share the applier as they always did; they skip the
/// pass while a committer leads the group. A committer that starts leading first stops new passes, then waits for the running ones to
/// end, and drives the applier alone until its leadership ends. Without a leadership nothing is excluded.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaDriverLease
{
    private readonly Lock _sync = new();
    private TaskCompletionSource? _drained;
    private bool _leading;
    private int _passes;

    /// <summary>Ends the leadership: passes may enter again.</summary>
    internal void EndLeading()
    {
        lock (_sync)
            _leading = false;
    }

    /// <summary>Ends a pass entered by <see cref="TryEnterPass" />; the last pass to end lets a waiting leadership start.</summary>
    internal void ExitPass()
    {
        TaskCompletionSource? drained = null;
        lock (_sync)
        {
            _passes--;
            if (_passes == 0)
            {
                drained = _drained;
                _drained = null;
            }
        }

        _ = drained?.TrySetResult();
    }

    /// <summary>Starts a leadership: no new pass enters, and the running passes are waited for.</summary>
    /// <param name="cancellationToken">Cancels the wait for the running passes; the leadership is then not started.</param>
    /// <returns>A task that completes once no pass runs and the caller drives the applier alone.</returns>
    /// <exception cref="ReplicaDriverLeaseHeldException">A leadership or a state transfer already drives the applier.</exception>
    internal async Task LeadAsync(CancellationToken cancellationToken)
    {
        Task drained;
        lock (_sync)
        {
            if (_leading)
                throw new ReplicaDriverLeaseHeldException("The applier is already driven by a leadership.");

            _leading = true;
            if (_passes == 0)
                return;

            _drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            drained = _drained.Task;
        }

        await WaitForPassesAsync(drained, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Starts a leadership unless one is already started, without waiting for another leadership to end.</summary>
    /// <param name="cancellationToken">Cancels the wait for the running passes; the leadership is then not started.</param>
    /// <returns><see langword="true" /> once no pass runs and the caller drives the applier alone; <see langword="false" /> when a leadership is already started.</returns>
    internal async Task<bool> TryLeadAsync(CancellationToken cancellationToken)
    {
        Task drained;
        lock (_sync)
        {
            if (_leading)
                return false;

            _leading = true;
            if (_passes == 0)
                return true;

            _drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            drained = _drained.Task;
        }

        await WaitForPassesAsync(drained, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>Enters a pass over the applier unless a leadership drives it.</summary>
    /// <returns><see langword="true" /> when the pass may run; the caller ends it with <see cref="ExitPass" />.</returns>
    internal bool TryEnterPass()
    {
        lock (_sync)
        {
            if (_leading)
                return false;

            _passes++;
            return true;
        }
    }

    /// <summary>Waits for the running passes to end; on cancellation the leadership just started is withdrawn.</summary>
    /// <param name="drained">Completes once the last running pass ended.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes once no pass runs.</returns>
    private async Task WaitForPassesAsync(Task drained, CancellationToken cancellationToken)
    {
        try
        {
            await drained.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            lock (_sync)
            {
                _leading = false;
                _drained = null;
            }

            throw;
        }
    }
}
