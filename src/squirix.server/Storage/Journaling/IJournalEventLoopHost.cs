using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Cross-thread surface the journal event loop needs from its owning coordinator. The coordinator owns
/// producer-shared atomics (queued-append counter, next sequence, durability waiter list, pipeline
/// failure) while the loop owns segment-write / roll / group-commit-flush state (audit item A2).
/// </summary>
internal interface IJournalEventLoopHost
{
    void CompleteDurabilityCheckpoint();

    void DecrementQueuedAppends();

    void FailPipeline(Exception reason);

    void PublishRoll(int targetSegmentIndex);

    void SetNextSequence(ulong value);

    void ThrowIfJournalThreadFailed();
}
