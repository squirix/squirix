using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by snapshot admission.</summary>
internal interface IJournalCoordinatorSnapshotState
{
    SemaphoreSlim MutationGate { get; }

    bool HasPendingMemoryApply();

    ValueTask WaitForPendingMemoryApplyDrainAsync(CancellationToken cancellationToken);
}
