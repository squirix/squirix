using System.Threading;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by the durability pipeline.</summary>
internal interface IJournalCoordinatorState : IJournalThreadState, IJournalStallProbeSource
{
    CancellationTokenSource BackgroundCancellation { get; }

    MutableInt32 DurabilityFlushScheduledFlag { get; }

    DurabilityAckRegistry DurabilityAcks { get; }

    JournalEventLoop EventLoop { get; }

    Ledger Ledger { get; }

    PersistenceOptions Options { get; }

    PendingAppendRegistry PendingAppends { get; }

    MutableInt32 QueuedAppendsCounter { get; }

    BoundedJournalRing Ring { get; }

    JournalDurabilityGroupCommit? GroupCommit { get; }
}
