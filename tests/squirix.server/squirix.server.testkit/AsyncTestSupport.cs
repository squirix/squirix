using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit;

/// <summary>Async polling helpers shared across the test suite.</summary>
public static class AsyncTestSupport
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(25);

    /// <summary>Polls <paramref name="condition"/> against <paramref name="state"/> until it holds, with the default budget.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Poll predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    public static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, CancellationToken cancellationToken) =>
        state.WaitUntilAsync(condition, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

    /// <summary>Polls <paramref name="condition"/> against <paramref name="state"/> until it holds or <paramref name="timeout"/> expires.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    public static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken) =>
        state.WaitUntilAsync(condition, timeout, TimeProvider.System, cancellationToken);

    /// <summary>Polls <paramref name="condition"/> until it holds or <paramref name="timeout"/> expires, using <paramref name="timeProvider"/>.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="timeProvider">Clock used for the deadline and poll interval.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> or <paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past <paramref name="timeout"/>.</exception>
    public static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentNullException.ThrowIfNull(timeProvider);
        return WaitUntilCoreAsync(state, condition, timeout, timeProvider, cancellationToken);
    }

    /// <summary>Polls the asynchronous <paramref name="condition"/> against <paramref name="state"/> until it holds, with the default budget.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Asynchronous poll predicate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    public static Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, CancellationToken cancellationToken) =>
        state.WaitUntilValueAsync(condition, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

    /// <summary>Polls the asynchronous <paramref name="condition"/> until it holds or <paramref name="timeout"/> expires.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Asynchronous poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    public static Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, TimeSpan timeout, CancellationToken cancellationToken) =>
        state.WaitUntilValueAsync(condition, timeout, TimeProvider.System, cancellationToken);

    /// <summary>Polls the asynchronous <paramref name="condition"/> until it holds or <paramref name="timeout"/> expires, using <paramref name="timeProvider"/>.</summary>
    /// <typeparam name="T">Observed state type.</typeparam>
    /// <param name="state">Observed state.</param>
    /// <param name="condition">Asynchronous poll predicate.</param>
    /// <param name="timeout">Maximum time to keep polling.</param>
    /// <param name="timeProvider">Clock used for the deadline and poll interval.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="condition"/> or <paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    /// <exception cref="TimeoutException">Thrown when the condition stays unsatisfied past <paramref name="timeout"/>.</exception>
    public static Task WaitUntilValueAsync<T>(
        this T state,
        Func<T, CancellationToken, ValueTask<bool>> condition,
        TimeSpan timeout,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentNullException.ThrowIfNull(timeProvider);
        return WaitUntilValueCoreAsync(state, condition, timeout, timeProvider, cancellationToken);
    }

    private static async Task WaitUntilCoreAsync<T>(T state, Func<T, bool> condition, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        var started = timeProvider.GetTimestamp();
        while (!condition(state))
        {
            var remaining = timeout - timeProvider.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            await Task.Delay(remaining < PollInterval ? remaining : PollInterval, timeProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WaitUntilValueCoreAsync<T>(
        T state,
        Func<T, CancellationToken, ValueTask<bool>> condition,
        TimeSpan timeout,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = new CancellationTokenSource(timeout, timeProvider);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
        while (true)
        {
            try
            {
                var satisfied = await condition(state, linkedSource.Token).ConfigureAwait(false);
                if (timeoutSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    throw new TimeoutException("Timed out waiting for the expected condition.");
                if (satisfied)
                    return;
            }
            catch (OperationCanceledException exception) when (timeoutSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for the expected condition.", exception);
            }

            cancellationToken.ThrowIfCancellationRequested();

            if (timeoutSource.IsCancellationRequested)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            try
            {
                await Task.Delay(PollInterval, timeProvider, linkedSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException exception) when (timeoutSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for the expected condition.", exception);
            }
        }
    }
}
