using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

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
    /// <param name="delay">The longest wait; zero or less does not wait.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when a publication happened after <paramref name="version" />; <see langword="false" /> on timeout.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<bool> WaitAsync(long version, TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        Task changed;
        lock (_sync)
        {
            if (_version != version)
                return true;

            if (delay <= TimeSpan.Zero)
                return false;

            _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            changed = _changed.Task;
        }

        using var timeout = new CancellationTokenSource(delay, timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        await changed.WaitAsync(linked.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        if (changed.IsCompleted)
            return true;

        cancellationToken.ThrowIfCancellationRequested();
        return false;
    }
}
