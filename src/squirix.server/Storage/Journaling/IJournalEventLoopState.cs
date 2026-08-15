namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable journal event-loop state used by journal-thread helpers.</summary>
internal interface IJournalEventLoopState
{
    long ActiveSegmentWrittenBytes { get; }

    JournalDurabilityGroupCommit? GroupCommit { get; }

    IJournalEventLoopHost Host { get; }

    long JournalTotalBytes { get; }

    PersistenceOptions Options { get; }

    JournalSegmentPolicy Policy { get; }

    IJournalSegmentWriter SegmentWriter { get; }

    JournalWriteBatchBuffer WriteBatch { get; }

    void AddJournalTotalBytes(long delta);

    void FsyncOnJournalThread();

    void SetActiveSegmentWrittenBytes(long value);

    void SetDirty(bool value);

    void SetJournalTotalBytes(long value);
}
