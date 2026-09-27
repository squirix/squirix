using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by the append pipeline.</summary>
internal interface IJournalCoordinatorAppendState
{
    JournalDurabilityGroupCommit? GroupCommit { get; }

    JournalDurabilityCoordinator DurabilityPipeline { get; }

    PersistenceOptions Options { get; }

    PendingAppendRegistry PendingAppends { get; }

    MutableInt32 QueuedAppendsCounter { get; }

    BoundedJournalRing Ring { get; }

    AsyncManualResetEvent StartupGate { get; }

    ulong AllocateSequence(in AsyncLockOwnership ownership);

    /// <summary>Refuses, before the frame is tracked or enqueued, an append the journal thread might reject for capacity.</summary>
    /// <param name="frameLength">Length of the encoded frame to admit.</param>
    /// <exception cref="Squirix.Server.Errors.JournalCapacityExceededException">The frame may exceed a journal capacity limit.</exception>
    void EnsureAppendAdmission(int frameLength);

    void RecordAppendMetrics(int frameLength, long startedMs);
}
