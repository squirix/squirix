using System;
using System.Collections;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Entries whose array and non-pooled payload memory have been handed over for immutable log retention.</summary>
[Immutable]
internal sealed class OwnedLogEntries : IReadOnlyList<FollowerLogEntry>
{
    private static readonly OwnedLogEntries Empty = new([]);
    private readonly FollowerLogEntry[] _entries;

    private OwnedLogEntries(FollowerLogEntry[] entries)
    {
        _entries = entries;
    }

    public int Count => _entries.Length;

    /// <summary>Gets the stable entry memory without materializing another entry array.</summary>
    internal ReadOnlyMemory<FollowerLogEntry> Memory => _entries;

    public FollowerLogEntry this[int index] => _entries[index];

    public IEnumerator<FollowerLogEntry> GetEnumerator()
    {
        foreach (var entry in _entries)
            yield return entry;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Takes over entries and immutable payload memory without copying.</summary>
    /// <param name="entries">The entry array whose payloads are immutable and non-pooled.</param>
    /// <returns>The owned view of the entries.</returns>
    /// <remarks>No caller may change the entry array or payload bytes afterwards, or return them to a pool.</remarks>
    internal static OwnedLogEntries TakeOwnership(FollowerLogEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return entries.Length == 0 ? Empty : new OwnedLogEntries(entries);
    }
}
