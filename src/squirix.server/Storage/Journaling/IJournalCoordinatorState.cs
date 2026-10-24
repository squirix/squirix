using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by the durability pipeline.</summary>
internal interface IJournalCoordinatorState : IJournalThreadState, IJournalStallProbeSource
{
    DurabilityAckRegistry DurabilityAcks { get; }

    MutableInt32 DurabilityFlushScheduledFlag { get; }

    JournalEventLoop EventLoop { get; }

    JournalDurabilityGroupCommit? GroupCommit { get; }

    Ledger Ledger { get; }

    PersistenceOptions Options { get; }

    PendingAppendRegistry PendingAppends { get; }

    MutableInt32 QueuedAppendsCounter { get; }

    BoundedJournalRing Ring { get; }
}
