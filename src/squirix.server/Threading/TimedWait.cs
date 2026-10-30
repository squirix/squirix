using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Waits for a condition until a delay elapses, treating the timeout as an ordinary outcome rather than an exception.</summary>
internal static class TimedWait
{
    /// <summary>The longest delay a timer accepts; a longer finite delay is clamped to it.</summary>
    internal static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static readonly Task<TimedWaitResult> SignaledTask = Task.FromResult(TimedWaitResult.Signaled);

    /// <summary>Waits for <paramref name="awaited" /> to complete, the delay to elapse or the token to cancel, without a timer when the task already completed.</summary>
    /// <param name="awaited">The task whose completion is the awaited condition.</param>
    /// <param name="delay">The longest wait; <see cref="Timeout.InfiniteTimeSpan" /> waits for the task or the cancellation only, and a value above <see cref="MaxDelay" /> is clamped to it.</param>
    /// <param name="timeProvider">The time source of the delay.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" /> unless the task already completed.</param>
    /// <returns><see cref="TimedWaitResult.Signaled" /> when the task completed, <see cref="TimedWaitResult.TimedOut" /> when the delay elapsed first, <see cref="TimedWaitResult.Canceled" /> when the token was canceled first.</returns>
    internal static Task<TimedWaitResult> WaitAsync(Task awaited, TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(awaited);
        ArgumentNullException.ThrowIfNull(timeProvider);

        // A task that already completed holds the condition, as a canceled token or an elapsed delay never overrides a held condition.
        return awaited.IsCompleted
            ? SignaledTask
            : WaitAsync(
                awaited,
                static (task, token) => task.WaitAsync(token),
                static task => task.IsCompleted,
                delay,
                timeProvider,
                cancellationToken);
    }

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
        using var scope = new CancellationTokenSource(delay > MaxDelay ? MaxDelay : delay, timeProvider);
        var parentRegistration = cancellationToken.UnsafeRegister(ForwardCancellation, scope);
        try
        {
            await start(state, scope.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

            // The delay is never reported as a cancellation: only the caller's own token counts.
            var unmet = cancellationToken.IsCancellationRequested ? TimedWaitResult.Canceled : TimedWaitResult.TimedOut;
            return isDone(state) ? TimedWaitResult.Signaled : unmet;
        }
        finally
        {
            await parentRegistration.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Forward synchronously, as a linked source does; asynchronous cancellation would change callback ordering.
#pragma warning disable MA0045
    private static void ForwardCancellation(object? state) => Unsafe.As<CancellationTokenSource>(state)!.Cancel();
#pragma warning restore MA0045
}
