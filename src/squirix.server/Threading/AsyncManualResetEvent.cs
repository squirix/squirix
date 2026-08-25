using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Async manual-reset event: a one-shot, resettable readiness latch.</summary>
/// <remarks>
/// Starts unset (not signaled). <see cref="Set" /> releases all current and future waiters.
/// Awaiting <see cref="WaitAsync" /> blocks until <see cref="Set" /> is called, then returns immediately for every
/// subsequent await. <see cref="Set" /> is idempotent.
/// <para>A fresh event models a node that is not ready until an explicit preparation step (recovery replay)
/// signals completion, matching the startup-gate contract in production.</para>
/// </remarks>
[ThreadSafe]
internal sealed class AsyncManualResetEvent
{
    private readonly TaskCompletionSource _tcs = TaskCompletionSourceFactory.Create();

    internal AsyncManualResetEvent(bool initialState = false)
    {
        if (initialState)
            Set();
    }

    /// <summary>Gets a value indicating whether the event has been signaled.</summary>
    internal bool IsSet => _tcs.Task.IsCompleted;

    /// <summary>Signals the event, releasing all current and future waiters. Idempotent.</summary>
    internal void Set() => _tcs.TrySetResult();

    /// <summary>Waits until the event is signaled.</summary>
    /// <param name="cancellationToken">Cancellation for the wait when the event is not yet signaled.</param>
    /// <returns>A <see cref="ValueTask" /> that completes when the event is signaled.</returns>
    internal ValueTask WaitAsync(CancellationToken cancellationToken = default)
    {
        var task = _tcs.Task;
        return !cancellationToken.CanBeCanceled || task.IsCompleted
            ? new ValueTask(task)
            : new ValueTask(task.WaitAsync(cancellationToken));
    }
}
