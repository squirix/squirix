using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Durability flush and pending in-memory apply coordination for journal-backed mutations.</summary>
internal interface IJournalDurabilityCoordinator
{
    QuiescenceGate InFlightApplyGate { get; }

    /// <summary>Waits until the caller's already write-acked frames are covered by a durability flush.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established.</returns>
    /// <remarks>
    /// Call it only after the frame's write ack completed, or, for barrier callers, over frames whose applies already returned. With group
    /// commit, a graceful stop completes the wait through its final flush, and a wait issued after that flush succeeds at once; a latched
    /// pipeline failure is refused first.
    /// </remarks>
    ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken);

    void FailJournalPipeline(Exception reason);
}
