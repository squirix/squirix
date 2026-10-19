using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Wakes a loop that waits with a timeout: the apply loop of one replica group when its log may hold committed entries memory has not received yet, or the first start of a group when a peer called.</summary>
/// <remarks>
/// Notifications coalesce: at most one is pending, and any number raised before the loop takes it count as one. A notification raised
/// while the loop runs a pass stays pending, so the loop runs one more pass and never misses an entry. Notifying never waits and never
/// throws: it runs on the replication RPC paths. The apply loop of the group is the single reader.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaApplySignal
{
    private readonly Lock _sync = new();
    private TaskCompletionSource? _waiter;
    private bool _pending;

    /// <summary>Marks the group as having entries to apply; a notification already pending absorbs this one.</summary>
    internal void Notify()
    {
        TaskCompletionSource? waiter;
        lock (_sync)
        {
            _pending = true;
            waiter = _waiter;
            _waiter = null;
        }

        _ = waiter?.TrySetResult();
    }

    /// <summary>Waits until a notification is pending or <paramref name="delay" /> elapses, then takes the pending notification.</summary>
    /// <param name="delay">The longest wait when nothing is pending.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing. A timeout is not an exception.</param>
    /// <returns><see langword="true" /> when a notification was pending; <see langword="false" /> when the delay elapsed first.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<bool> WaitAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        Task woken;
        lock (_sync)
        {
            if (_pending)
            {
                _pending = false;
                return true;
            }

            _waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            woken = _waiter.Task;
        }

        if (!await TimedWait.WaitAsync(
                woken,
                static (task, token) => task.WaitAsync(token),
                static task => task.IsCompleted,
                delay,
                timeProvider,
                cancellationToken).ConfigureAwait(false))
            return false;

        lock (_sync)
        {
            var taken = _pending;
            _pending = false;
            return taken;
        }
    }
}
