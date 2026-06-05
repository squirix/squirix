using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Triggers on-demand journal compaction from admin REST endpoints.
/// </summary>
public interface IAdminJournalCompactionTrigger
{
    /// <summary>
    /// Attempts to start journal compaction immediately.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when compaction started; otherwise <see langword="false" />.</returns>
    ValueTask<bool> TryTriggerCompactionAsync(CancellationToken cancellationToken);
}
