using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Read-only source of replica-group status snapshots for diagnostics.</summary>
internal interface IReplicaStatusSource
{
    /// <summary>Gets the current snapshot of every served replica group without mutating replication state.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The per-group snapshots; empty when replication is not configured.</returns>
    ValueTask<IReadOnlyList<ReplicaStatusSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken);
}
