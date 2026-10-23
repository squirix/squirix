using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Waits for a condition until a delay elapses, treating the timeout as an ordinary outcome rather than an exception.</summary>
[Immutable]
internal static class TimedWait
{
    /// <summary>The longest delay a timer accepts; a longer finite delay is clamped to it.</summary>
    internal static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>Waits until the wait ends, the delay elapses or the token is canceled, without throwing on the timeout or the cancellation.</summary>
    /// <typeparam name="TState">The state passed to both delegates, so they can be <see langword="static" />.</typeparam>
    /// <param name="state">The state passed to <paramref name="start" /> and <paramref name="isDone" />.</param>
    /// <param name="start">Starts the wait; the task it returns must end when the token it receives is canceled.</param>
    /// <param name="isDone">Tells whether the awaited condition holds once the wait ended; it may consume the condition.</param>
    /// <param name="delay">The longest wait; <see cref="Timeout.InfiniteTimeSpan" /> waits for the condition or the cancellation only, and a value above <see cref="MaxDelay" /> is clamped to it.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" /> unless the condition already holds.</param>
    /// <returns><see cref="TimedWaitResult.Signaled" /> when the condition holds, <see cref="TimedWaitResult.TimedOut" /> when the delay elapsed first, <see cref="TimedWaitResult.Canceled" /> when the token was canceled first.</returns>
    internal static async Task<TimedWaitResult> WaitAsync<TState>(
        TState state,
        Func<TState, CancellationToken, Task> start,
        Func<TState, bool> isDone,
        TimeSpan delay,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(start);
        ArgumentNullException.ThrowIfNull(isDone);
        ArgumentNullException.ThrowIfNull(timeProvider);

        // A timer accepts the infinite delay as is; a finite delay above its limit is clamped.
        using var timeout = new CancellationTokenSource(delay > MaxDelay ? MaxDelay : delay, timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        await start(state, linked.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // The delay is never reported as a cancellation: only the caller's own token counts.
        var unmet = cancellationToken.IsCancellationRequested ? TimedWaitResult.Canceled : TimedWaitResult.TimedOut;
        return isDone(state) ? TimedWaitResult.Signaled : unmet;
    }
}
