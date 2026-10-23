using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Replication;

/// <summary>Leader-side commit gate mirroring the protocol-model election safety rules.</summary>
/// <remarks>
/// A majority that replicates only entries from older terms must wait: an entry becomes committable only once
/// the leader log holds a current-term entry at or before the candidate index, so committing it transitively
/// commits the older prefix. This mirrors <c language="csharp">HasCurrentTermEntryThrough</c> of the protocol model.
/// </remarks>
internal static class ElectionCommitRule
{
    /// <summary>Determines whether the leader log holds a current-term entry at or before <paramref name="index" />.</summary>
    /// <param name="entries">The leader log entries in index order.</param>
    /// <param name="currentTerm">The leader current term.</param>
    /// <param name="index">The commit candidate index.</param>
    /// <returns><see langword="true" /> when a current-term entry reaches the candidate index.</returns>
    internal static bool HasCurrentTermEntryThrough(IReadOnlyList<FollowerLogEntry> entries, ulong currentTerm, ulong index)
    {
        ArgumentNullException.ThrowIfNull(entries);
        for (var i = 0; i < entries.Count; i++)
        {
            if (entries[i].Term == currentTerm && entries[i].LogIndex <= index)
                return true;
        }

        return false;
    }
}
