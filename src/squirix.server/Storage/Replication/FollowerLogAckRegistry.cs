using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Replication;

/// <summary>Tracks the acknowledgments of follower-log durable operations whose work item was scheduled on a pool thread.</summary>
/// <remarks>
/// An acknowledgment is tracked right before its work item is scheduled and is untracked by the pool thread after it wrote the
/// outcome of the operation, or by the shutdown drain, which faults whatever is still in flight. There is no pending state: the
/// caller never removes a scheduled acknowledgment. Completion goes through <see cref="TaskCompletionSource.TrySetResult()" /> and
/// its siblings, so the first writer wins and a late outcome after the drain is a no-op.
/// </remarks>
[ThreadSafe]
internal sealed class FollowerLogAckRegistry
{
    private readonly Lock _sync = new();
    private Exception? _failure;
    private List<TaskCompletionSource> _inFlight = [];

    /// <summary>Releases an acknowledgment once the pool thread wrote the outcome of its operation; a no-op after the drain.</summary>
    /// <param name="ack">Acknowledgment previously passed to <see cref="Track" />.</param>
    internal void Complete(TaskCompletionSource ack)
    {
        lock (_sync)
            _ = _inFlight.Remove(ack);
    }

    /// <summary>Faults every tracked acknowledgment with <paramref name="failure" /> and closes the registry.</summary>
    /// <param name="failure">Failure reported to the in-flight waiters and to every later <see cref="Track" /> call.</param>
    /// <returns>The number of waiters this call faulted; an acknowledgment whose outcome was already written is not counted.</returns>
    internal int FaultAll(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        List<TaskCompletionSource> taken;
        Exception recorded;
        lock (_sync)
        {
            _failure ??= failure;
            recorded = _failure;
            taken = _inFlight;
            _inFlight = [];
        }

        return taken.FaultPending(recorded);
    }

    /// <summary>Tracks an acknowledgment before its work item is scheduled.</summary>
    /// <param name="ack">Acknowledgment completed by the pool thread or faulted by the drain.</param>
    /// <exception cref="ObjectDisposedException">The registry was drained; the recorded failure is rethrown so no waiter parks on an operation nobody releases.</exception>
    internal void Track(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        lock (_sync)
        {
            if (_failure != null)
                ExceptionDispatchInfo.Capture(_failure).Throw();

            _inFlight.Add(ack);
        }
    }
}
