using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Read-index wait for leader reads: a read is served only after the applied index reaches the read index.</summary>
internal static class LeaderReadBarrier
{
    /// <summary>Waits until the observed applied index reaches the read index.</summary>
    /// <param name="observeAppliedIndex">Reads the current applied index without taking durable gates.</param>
    /// <param name="readIndex">The read index the read must observe.</param>
    /// <param name="timeProvider">The time source driving the poll delay.</param>
    /// <param name="pollInterval">The delay between applied-index observations; must be positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the read may be served.</returns>
    internal static Task WaitUntilAppliedAsync(
        Func<ulong> observeAppliedIndex,
        ulong readIndex,
        TimeProvider timeProvider,
        TimeSpan pollInterval,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(observeAppliedIndex);
        return WaitUntilAppliedAsync(observeAppliedIndex, static observe => observe(), readIndex, (timeProvider, pollInterval), cancellationToken);
    }

    /// <summary>Waits until the applied index observed through a state reaches the read index, without a capturing callback.</summary>
    /// <typeparam name="TState">The type of the state handed to <paramref name="observeAppliedIndex" />.</typeparam>
    /// <param name="state">The state handed to <paramref name="observeAppliedIndex" />.</param>
    /// <param name="observeAppliedIndex">Reads the current applied index from the state without taking durable gates.</param>
    /// <param name="readIndex">The read index the read must observe.</param>
    /// <param name="poll">The time source driving the poll delay, and the delay between applied-index observations; it must be positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the read may be served.</returns>
    internal static async Task WaitUntilAppliedAsync<TState>(
        TState state,
        Func<TState, ulong> observeAppliedIndex,
        ulong readIndex,
        (TimeProvider TimeProvider, TimeSpan Interval) poll,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(observeAppliedIndex);
        ArgumentNullException.ThrowIfNull(poll.TimeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(poll.Interval, TimeSpan.Zero);

        var (timeProvider, pollInterval) = poll;
        while (observeAppliedIndex(state) < readIndex)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(pollInterval, timeProvider, cancellationToken).ConfigureAwait(false);
        }
    }
}
