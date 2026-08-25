using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Serializes journal maintenance work (for example, compaction) with the same exclusivity rules as the pipelined journal coordinator.</summary>
/// <remarks>
/// Implemented by <see cref="IJournalCoordinator" /> and journal coordinators so hosted compaction depends on this narrow surface.
/// </remarks>
internal interface IExclusiveMaintenanceExecutor
{
    /// <summary>
    /// Runs <paramref name="action" /> while holding the journal maintenance gates used for compaction and segment rotation.
    /// </summary>
    /// <param name="action">The maintenance work to execute.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A value task that completes when maintenance finishes.</returns>
    ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken);
}
