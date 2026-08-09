namespace Squirix.Server.Storage.Replication;

/// <summary>Storage surface for the in-memory and persisted follower log state.</summary>
internal interface IFollowerLogStorage
{
    /// <summary>Gets the snapshot baseline describing the compacted base entry.</summary>
    SnapshotBaseline SnapshotBaseline { get; }

    /// <summary>Gets a read-only, ascending-order view of the in-memory log entries keyed by index.</summary>
    /// <remarks>
    /// Keys stay aligned with <see cref="EntryOffsets" /> and must stay above the snapshot baseline.
    /// Mutation happens only through the paired mutators below, so no coordinator can desynchronize
    /// the two indexes by writing to one map alone.
    /// </remarks>
    ReadOnlySortedDictionary<ulong, FollowerLogEntry> Entries { get; }

    /// <summary>Gets a read-only, ascending-order view of the persisted entry offsets and terms keyed by index.</summary>
    /// <remarks>Offsets reference the current log file and are removed together with the matching <see cref="Entries" /> key.</remarks>
    ReadOnlySortedDictionary<ulong, (long Offset, ulong Term)> EntryOffsets { get; }

    /// <summary>Gets the follower log file paths.</summary>
    FollowerLogPaths Paths { get; }

    /// <summary>Gets the group snapshot store.</summary>
    IFollowerLogSnapshotStore Snapshot { get; }

    /// <summary>Adds an entry to both indexes as one paired mutation.</summary>
    /// <param name="entry">The entry payload retained above the applied watermark.</param>
    /// <param name="offset">The frame offset in the current log file.</param>
    /// <param name="term">The entry term recorded with the offset.</param>
    void AddEntry(FollowerLogEntry entry, long offset, ulong term);

    /// <summary>
    /// Records only an offset and term for an index whose payload was released from memory. Reserved for
    /// recovery of frames at or below the applied watermark: the offset stays available for durable truncation
    /// and term verification while <see cref="Entries" /> intentionally gains no key.
    /// </summary>
    /// <param name="index">The log index of the frame.</param>
    /// <param name="offset">The frame offset in the current log file.</param>
    /// <param name="term">The frame term.</param>
    void AddEntryOffset(ulong index, long offset, ulong term);

    /// <summary>Removes every entry and its matching offset at indexes strictly above <paramref name="index" /> from both maps.</summary>
    /// <param name="index">The highest index to retain.</param>
    void RemoveEntriesAbove(ulong index);

    /// <summary>Removes every entry and its matching offset at or below <paramref name="index" /> from both maps.</summary>
    /// <param name="index">The highest index to remove; every retained index is strictly above it.</param>
    void RemoveEntriesThrough(ulong index);

    /// <summary>
    /// Releases the payloads at or below <paramref name="index" /> from <see cref="Entries" /> only. The offsets
    /// deliberately survive so a divergent tail can still be truncated durably and applied-region term conflicts
    /// can still be detected; this is the one sanctioned unpaired removal.
    /// </summary>
    /// <param name="index">The applied watermark whose covered payloads are released.</param>
    void ReleaseAppliedEntries(ulong index);

    /// <summary>Clears both indexes as one paired mutation before they are rebuilt.</summary>
    void ClearEntries();

    /// <summary>Advances the baseline and removes every retained entry at or below its last included index.</summary>
    /// <param name="baseline">The new snapshot baseline.</param>
    void AdvanceBaseline(SnapshotBaseline baseline);
}
