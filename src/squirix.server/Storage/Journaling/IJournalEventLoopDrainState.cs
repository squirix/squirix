using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable journal event-loop state used by the drain scheduler.</summary>
internal interface IJournalEventLoopDrainState
{
    CancellationToken BackgroundToken { get; }

    JournalDurabilityGroupCommit? GroupCommit { get; }

    IJournalEventLoopHost Host { get; }

    BoundedJournalRing Ring { get; }
}
