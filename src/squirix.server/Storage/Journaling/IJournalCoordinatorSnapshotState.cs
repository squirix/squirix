using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by snapshot admission.</summary>
internal interface IJournalCoordinatorSnapshotState
{
    AsyncLock MutationGate { get; }

    QuiescenceGate InFlightApplyGate { get; }
}
