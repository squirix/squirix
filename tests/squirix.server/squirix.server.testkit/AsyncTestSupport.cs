using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit;

/// <summary>Async polling helpers shared across the test suite.</summary>
public static class AsyncTestSupport
{
    /// <summary>Polls <paramref name="condition"/> against <paramref name="state"/> until it holds, with the default budget.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Poll predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> is <see langword="null" />.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past the default budget.</exception>
    public static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, CancellationToken cancellationToken) =>
        state.WaitUntilAsync(condition, TimeSpan.FromSeconds(5), cancellationToken);

    /// <summary>Polls the asynchronous <paramref name="condition"/> against <paramref name="state"/> until it holds, with the default budget.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Asynchronous poll predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> is <see langword="null" />.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past the default budget.</exception>
    public static Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, CancellationToken cancellationToken) =>
        state.WaitUntilValueAsync(condition, TimeSpan.FromSeconds(5), cancellationToken);

    /// <summary>Polls <paramref name="condition"/> against <paramref name="state"/> until it holds or <paramref name="timeout"/> expires.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> is <see langword="null" />.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past <paramref name="timeout"/>.</exception>
    public static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);
        return WaitUntilCoreAsync(state, condition, timeout, cancellationToken);
    }

    /// <summary>Polls the asynchronous <paramref name="condition"/> against <paramref name="state"/> until it holds or <paramref name="timeout"/> expires.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Asynchronous poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> is <see langword="null" />.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past <paramref name="timeout"/>.</exception>
    public static Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);
        return WaitUntilValueCoreAsync(state, condition, timeout, cancellationToken);
    }

    private static async Task WaitUntilCoreAsync<T>(T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (!condition(state))
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WaitUntilValueCoreAsync<T>(T state, Func<T, CancellationToken, ValueTask<bool>> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (true)
        {
            var remainingMs = deadline - Environment.TickCount64;
            if (remainingMs <= 0)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            using var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            source.CancelAfter(TimeSpan.FromMilliseconds(remainingMs));

            try
            {
                var satisfied = await condition(state, source.Token).ConfigureAwait(false);
                if (satisfied)
                    return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for the expected condition.");
            }

            remainingMs = deadline - Environment.TickCount64;
            if (remainingMs <= 0)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            var delayMs = remainingMs < 25 ? Convert.ToInt32(remainingMs) : 25;
            await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
        }
    }
}
