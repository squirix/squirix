using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.UnitTests;

/// <summary>Async polling helpers shared across the test suite.</summary>
internal static class AsyncTestSupport
{
    internal static Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, CancellationToken cancellationToken) =>
        state.WaitUntilAsync(condition, TimeSpan.FromSeconds(5), cancellationToken);

    internal static Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, CancellationToken cancellationToken) =>
        state.WaitUntilValueAsync(condition, TimeSpan.FromSeconds(5), cancellationToken);

    internal static async Task WaitUntilAsync<T>(this T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (!condition(state))
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("Timed out waiting for the expected condition.");

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    internal static async Task WaitUntilValueAsync<T>(this T state, Func<T, CancellationToken, ValueTask<bool>> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);

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
