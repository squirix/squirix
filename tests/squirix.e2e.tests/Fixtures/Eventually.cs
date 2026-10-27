using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.E2ETests.Fixtures;

/// <summary>
/// Retries an SDK call while the cluster is recovering: a call that fails with <see cref="StatusCode.Unavailable" /> or
/// <see cref="StatusCode.DeadlineExceeded" /> is tried again until it succeeds or the bound is spent; every attempt is recorded.
/// </summary>
/// <remarks>Any other failure ends the retry at once and is rethrown, so a wrong answer is never retried away.</remarks>
internal static class Eventually
{
    /// <summary>The pause between two attempts, so a call that fails at once does not spin.</summary>
    private static readonly TimeSpan Backoff = TimeSpan.FromMilliseconds(100);

    /// <summary>Runs a call until it succeeds, retrying unavailable and deadline failures within a bound.</summary>
    /// <typeparam name="TState">The type of the state the call reads.</typeparam>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="state">The state the call reads.</param>
    /// <param name="operation">The call; it receives a token canceled at the bound.</param>
    /// <param name="bound">The longest time all attempts together may take.</param>
    /// <param name="attempts">Receives one entry per attempt, the last one included.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The result of the first successful attempt.</returns>
    /// <exception cref="TimeoutException">No attempt succeeded within the bound; the message lists every attempt.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal static async Task<T> SucceedsAsync<TState, T>(
        TState state,
        Func<TState, CancellationToken, Task<T>> operation,
        TimeSpan bound,
        List<EventualAttempt> attempts,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(attempts);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(bound, TimeSpan.Zero);
        var started = Stopwatch.GetTimestamp();
        using var limit = new CancellationTokenSource(bound);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, limit.Token);
        while (true)
        {
            var attemptStarted = Stopwatch.GetElapsedTime(started);
            try
            {
                var result = await operation(state, linked.Token);
                attempts.Add(new EventualAttempt(attemptStarted, Stopwatch.GetElapsedTime(started) - attemptStarted, "ok"));
                return result;
            }
            catch (Exception exception)
            {
                attempts.Add(new EventualAttempt(attemptStarted, Stopwatch.GetElapsedTime(started) - attemptStarted, Describe(exception)));
                if (limit.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    throw BoundSpent(bound, attempts, exception);

                if (exception is not RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded })
                    throw;
            }

            try
            {
                await Task.Delay(Backoff, TimeProvider.System, linked.Token);
            }
            catch (OperationCanceledException exception) when (limit.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw BoundSpent(bound, attempts, exception);
            }
        }
    }

    /// <summary>Runs a call until it succeeds, retrying unavailable and deadline failures within a bound.</summary>
    /// <typeparam name="TState">The type of the state the call reads.</typeparam>
    /// <param name="state">The state the call reads.</param>
    /// <param name="operation">The call; it receives a token canceled at the bound.</param>
    /// <param name="bound">The longest time all attempts together may take.</param>
    /// <param name="attempts">Receives one entry per attempt, the last one included.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once an attempt succeeded.</returns>
    /// <exception cref="TimeoutException">No attempt succeeded within the bound; the message lists every attempt.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal static Task SucceedsAsync<TState>(
        TState state,
        Func<TState, CancellationToken, Task> operation,
        TimeSpan bound,
        List<EventualAttempt> attempts,
        CancellationToken cancellationToken) =>
        SucceedsAsync(
            (State: state, Operation: operation),
            static async (call, token) =>
            {
                await call.Operation(call.State, token);
                return true;
            },
            bound,
            attempts,
            cancellationToken);

    /// <summary>Lists attempts one per line, for assertion messages.</summary>
    /// <param name="attempts">The attempts.</param>
    /// <returns>The attempts, one per line.</returns>
    internal static string Dump(IReadOnlyList<EventualAttempt> attempts)
    {
        ArgumentNullException.ThrowIfNull(attempts);
        var text = new StringBuilder();
        for (var i = 0; i < attempts.Count; i++)
        {
            var attempt = attempts[i];
            _ = text.Append(
                CultureInfo.InvariantCulture,
                $"attempt {i + 1}: at {attempt.Started.TotalMilliseconds:F0} ms, ran {attempt.Elapsed.TotalMilliseconds:F0} ms, {attempt.Outcome}").AppendLine();
        }

        return text.ToString();
    }

    private static TimeoutException BoundSpent(TimeSpan bound, List<EventualAttempt> attempts, Exception last) =>
        new(string.Create(CultureInfo.InvariantCulture, $"No attempt succeeded within {bound.TotalSeconds:F1} s:{Environment.NewLine}{Dump(attempts)}"), last);

    private static string Describe(Exception exception) => exception is RpcException rpc ? $"{nameof(RpcException)} {rpc.StatusCode}" : exception.GetType().Name;
}
