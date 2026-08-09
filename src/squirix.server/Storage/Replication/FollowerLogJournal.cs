using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>
/// Paired in-memory journal state of one replica group: the entry payloads, their durable frame offsets, and
/// the snapshot baseline. Owns every paired mutation of the two indexes so no coordinator can desynchronize them.
/// </summary>
/// <remarks>
/// Callers hold the owning log's gate; the journal performs no synchronization of its own. All mutations happen
/// only through the mutator methods below.
/// </remarks>
[Immutable]
internal sealed class FollowerLogJournal : IFollowerLogStorage
{
    private readonly SortedDictionary<ulong, FollowerLogEntry> _entries = [];
    private readonly SortedDictionary<ulong, (long Offset, ulong Term)> _entryOffsets = [];
    private SnapshotBaseline _snapshotBaseline = new(0UL, 0UL);

    internal FollowerLogJournal(FollowerLogPaths paths, GroupSnapshotStore snapshot)
    {
        Paths = paths;
        Snapshot = snapshot;
    }

    /// <inheritdoc />
    public ReadOnlySortedDictionary<ulong, FollowerLogEntry> Entries => new(_entries);

    /// <inheritdoc />
    public ReadOnlySortedDictionary<ulong, (long Offset, ulong Term)> EntryOffsets => new(_entryOffsets);

    /// <inheritdoc />
    public SnapshotBaseline SnapshotBaseline => _snapshotBaseline;

    /// <inheritdoc />
    public FollowerLogPaths Paths { get; }

    /// <inheritdoc />
    public IFollowerLogSnapshotStore Snapshot { get; }

    /// <inheritdoc />
    public void AddEntry(FollowerLogEntry entry, long offset, ulong term)
    {
        // Callers hold _gate; the entry and its offset are written as one paired mutation.
        _entryOffsets[entry.LogIndex] = (offset, term);
        _entries[entry.LogIndex] = entry;
    }

    /// <inheritdoc />
    public void AddEntryOffset(ulong index, long offset, ulong term) => _entryOffsets[index] = (offset, term);

    /// <inheritdoc />
    public void RemoveEntriesAbove(ulong index) => RemoveEntriesAboveCore(index);

    /// <inheritdoc />
    public void RemoveEntriesThrough(ulong index) => RemoveEntriesThroughCore(index);

    /// <inheritdoc />
    public void ReleaseAppliedEntries(ulong index)
    {
        // Callers hold _gate; the offsets deliberately survive so durable truncation and applied-region
        // term verification keep working after the payloads are released.
        var released = new List<ulong>();
        foreach (var key in _entries.Keys)
        {
            if (key <= index)
                released.Add(key);
        }

        for (var i = 0; i < released.Count; i++)
            _ = _entries.Remove(released[i]);
    }

    /// <inheritdoc />
    public void ClearEntries()
    {
        // Callers hold _gate; both indexes reset together before a rebuild repopulates them.
        _entries.Clear();
        _entryOffsets.Clear();
    }

    /// <inheritdoc />
    public void AdvanceBaseline(SnapshotBaseline baseline)
    {
        // Callers hold _gate; this is the single paired mutation of the baseline and both indexes.
        RemoveEntriesThroughCore(baseline.LastIncludedIndex);
        _snapshotBaseline = baseline;
    }

    /// <summary>
    /// Installs the snapshot baseline without pruning the retained indexes. Reserved for paths whose index
    /// lifecycle is owned separately: snapshot publication retains the covered prefix until the applied watermark
    /// releases it, while recovery and installation rebuild both indexes from the retained tail afterward.
    /// </summary>
    /// <param name="baseline">The restored snapshot baseline.</param>
    internal void RestoreBaseline(SnapshotBaseline baseline) => _snapshotBaseline = baseline;

    /// <summary>Reads the entry payload at <paramref name="index" /> without materializing a read-only wrapper.</summary>
    /// <param name="index">The journal index to look up.</param>
    /// <param name="entry">The stored entry when present.</param>
    /// <returns><see langword="true" /> when the index carries a payload.</returns>
    internal bool TryGetEntry(ulong index, out FollowerLogEntry entry) => _entries.TryGetValue(index, out entry);

    /// <summary>Reads the durable frame location at <paramref name="index" /> without materializing a read-only wrapper.</summary>
    /// <param name="index">The journal index to look up.</param>
    /// <param name="location">The frame offset and term when present.</param>
    /// <returns><see langword="true" /> when the index carries retained frame metadata.</returns>
    internal bool TryGetEntryOffset(ulong index, out (long Offset, ulong Term) location) => _entryOffsets.TryGetValue(index, out location);

    /// <summary>Removes every entry and its matching offset at indexes strictly above <paramref name="index" />.</summary>
    /// <remarks>
    /// The removal is driven from the offset index because it is the superset: applied frames may hold an offset
    /// without a payload, so enumerating entries alone would leave stale offsets readable by term lookups.
    /// </remarks>
    /// <param name="index">The highest index to retain.</param>
    private void RemoveEntriesAboveCore(ulong index)
    {
        var truncated = new List<ulong>();
        foreach (var key in _entryOffsets.Keys)
        {
            if (key > index)
                truncated.Add(key);
        }

        foreach (var key in _entries.Keys)
        {
            if (key > index && !_entryOffsets.ContainsKey(key))
                truncated.Add(key);
        }

        for (var i = 0; i < truncated.Count; i++)
        {
            _ = _entries.Remove(truncated[i]);
            _ = _entryOffsets.Remove(truncated[i]);
        }
    }

    /// <summary>Removes every entry and its matching offset at or below <paramref name="index" />.</summary>
    /// <remarks>Driven from the offset superset for the same reason as <see cref="RemoveEntriesAboveCore" />.</remarks>
    /// <param name="index">The lowest index to retain above.</param>
    private void RemoveEntriesThroughCore(ulong index)
    {
        var expired = new List<ulong>();
        foreach (var key in _entryOffsets.Keys)
        {
            if (key <= index)
                expired.Add(key);
        }

        foreach (var key in _entries.Keys)
        {
            if (key <= index && !_entryOffsets.ContainsKey(key))
                expired.Add(key);
        }

        for (var i = 0; i < expired.Count; i++)
        {
            _ = _entries.Remove(expired[i]);
            _ = _entryOffsets.Remove(expired[i]);
        }
    }
}
