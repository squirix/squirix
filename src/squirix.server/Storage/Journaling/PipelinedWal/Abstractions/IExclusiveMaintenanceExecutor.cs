using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Serializes journal maintenance work (for example compaction) with the same exclusivity rules as <see cref="JournalWriter.ExecuteMaintenanceExclusiveAsync" />.
/// </summary>
/// <remarks>
/// Implemented by <see cref="IJournalCoordinator" /> so hosted compaction depends on this narrow surface instead of the full writer type.
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
