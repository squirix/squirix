using System;
using System.Threading;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable coordinator state used by the durability pipeline.</summary>
internal interface IJournalCoordinatorState
{
    CancellationTokenSource BackgroundCancellation { get; }

    MutableInt32 DurabilityFlushScheduledFlag { get; }

    JournalDurabilityWaiterRegistry DurabilityWaiters { get; }

    JournalEventLoop EventLoop { get; }

    Thread JournalThread { get; }

    ref Exception? JournalThreadFailureField { get; }

    Ledger Ledger { get; }

    PersistenceOptions Options { get; }

    BoundedJournalRing Ring { get; }

    JournalDurabilityGroupCommit? GroupCommit { get; }
}
