using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by the append pipeline.</summary>
internal interface IJournalCoordinatorAppendState
{
    JournalDurabilityGroupCommit? GroupCommit { get; }

    JournalDurabilityCoordinator DurabilityPipeline { get; }

    DurabilityAckRegistry DurabilityAcks { get; }

    PersistenceOptions Options { get; }

    MutableInt32 QueuedAppendsCounter { get; }

    BoundedJournalRing Ring { get; }

    AsyncManualResetEvent StartupGate { get; }

    ulong AllocateSequence();

    void RecordAppendMetrics(int frameLength, long startedMs);
}
