using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Tells every waiter that the leader route of one replica group may have changed.</summary>
/// <remarks>
/// A broadcast, not a queue: each publication bumps a version and wakes every waiter at once. A waiter reads <see cref="Version" />
/// before it checks the route, then waits from that version, so a publication between the check and the wait is never missed.
/// Publishing never waits, never throws, and allocates nothing while nobody waits: it runs under the election state lock.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaRouteSignal
{
    /// <summary>The longest delay a timer accepts; a longer finite delay is clamped to it.</summary>
    internal static readonly TimeSpan MaxDelay = TimedWait.MaxDelay;

    private readonly Lock _sync = new();
    private TaskCompletionSource? _changed;
    private long _version;

    /// <summary>Gets the number of publications so far.</summary>
    internal long Version
    {
        get
        {
            lock (_sync)
                return _version;
        }
    }

    /// <summary>Wakes every waiter; later waits from an earlier version end at once.</summary>
    internal void Publish()
    {
        TaskCompletionSource? changed;
        lock (_sync)
        {
            _version++;
            changed = _changed;
            _changed = null;
        }

        _ = changed?.TrySetResult();
    }

    /// <summary>Waits until a publication after <paramref name="version" /> or until <paramref name="delay" /> elapses.</summary>
    /// <param name="version">The version the caller saw before it checked the route.</param>
    /// <param name="delay">
    /// The longest wait: <see cref="Timeout.InfiniteTimeSpan" /> waits for a publication or the cancellation only; any other value of zero
    /// or less does not wait; a value above <see cref="MaxDelay" /> is clamped to it.
    /// </param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" />.</param>
    /// <returns>
    /// <see cref="TimedWaitResult.Signaled" /> when a publication happened after <paramref name="version" />; <see cref="TimedWaitResult.TimedOut" />
    /// on timeout; <see cref="TimedWaitResult.Canceled" /> when the token was canceled first.
    /// </returns>
    internal async Task<TimedWaitResult> WaitAsync(long version, TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        Task changed;
        lock (_sync)
        {
            if (_version != version)
                return TimedWaitResult.Signaled;

            if (delay <= TimeSpan.Zero && delay != Timeout.InfiniteTimeSpan)
                return TimedWaitResult.TimedOut;

            _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            changed = _changed.Task;
        }

        return await TimedWait.WaitAsync(
            changed,
            static (task, token) => task.WaitAsync(token),
            static task => task.IsCompleted,
            delay,
            timeProvider,
            cancellationToken).ConfigureAwait(false);
    }
}
