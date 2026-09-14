using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Cross-thread surface the journal event loop needs from its owning coordinator. The coordinator owns
/// producer-shared atomics (queued-append counter, next sequence, durability ack list, pipeline
/// failure) while the loop owns segment-write / roll / group-commit-flush state (audit item A2).
/// </summary>
internal interface IJournalEventLoopHost
{
    /// <summary>
    /// Gets the registry of admitted appends. Invariant: every append/marked item dequeued from the
    /// ring was tracked here before enqueue, so a missing entry always means a failure drain took it.
    /// </summary>
    PendingAppendRegistry PendingAppends { get; }

    void CompleteDurabilityCheckpoint(JournalWorkItem item);

    void DecrementQueuedAppends();

    void FailPipeline(Exception reason);

    void PublishRoll(int targetSegmentIndex);

    void SetNextSequence(ulong value);

    void ThrowIfJournalThreadFailed();
}
