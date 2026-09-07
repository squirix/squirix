using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Read-index wait for leader reads: a read is served only after the applied index reaches the read index.</summary>
[SuppressMessage("Usage", "MA0182:Internal type is apparently never used", Justification = "Test-only activation seam until failover activation wires the read barrier in a follow-up milestone.")]
internal static class LeaderReadBarrier
{
    /// <summary>Waits until the observed applied index reaches the read index.</summary>
    /// <param name="observeAppliedIndex">Reads the current applied index without taking durable gates.</param>
    /// <param name="readIndex">The read index the read must observe.</param>
    /// <param name="timeProvider">The time source driving the poll delay.</param>
    /// <param name="pollInterval">The delay between applied-index observations; must be positive.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the read may be served.</returns>
    internal static async Task WaitUntilAppliedAsync(
        Func<ulong> observeAppliedIndex,
        ulong readIndex,
        TimeProvider timeProvider,
        TimeSpan pollInterval,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(observeAppliedIndex);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(pollInterval, TimeSpan.Zero);

        while (observeAppliedIndex() < readIndex)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(pollInterval, timeProvider, cancellationToken).ConfigureAwait(false);
        }
    }
}
