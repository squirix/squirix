using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Provides deterministic fault injection at durable commit boundaries.</summary>
internal interface IReplicaCommitFaultHooks
{
    /// <summary>Observes one completed pipeline boundary.</summary>
    /// <param name="stage">Completed boundary.</param>
    /// <param name="mutation">Prepared mutation.</param>
    /// <param name="cancellationToken">Absolute-deadline cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken);
}
