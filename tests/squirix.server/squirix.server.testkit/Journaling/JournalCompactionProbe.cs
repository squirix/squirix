using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Read-only helpers for inspecting Put coverage in on-disk journals.</summary>
public static class JournalCompactionProbe
{
    /// <summary>Locates the latest Put for a key in journal segments at or above <paramref name="fromSegment" />.</summary>
    /// <param name="dataDir">Node persistence directory.</param>
    /// <param name="cacheNamespace">Cache namespace of the probe key.</param>
    /// <param name="probeKey">Key string to locate.</param>
    /// <param name="fromSegment">First journal segment index to scan.</param>
    /// <returns>Whether the key was found and the highest matching sequence.</returns>
    public static (bool Found, ulong LastSequence) FindKeyInJournal(
        string dataDir,
        string cacheNamespace,
        string probeKey,
        int fromSegment = 1)
    {
        ulong lastSeq = 0;
        var found = false;
        using var records = JournalReadPath.ReadAll(dataDir, fromSegment, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is not JournalOperationKind.Put)
                continue;

            if (!string.Equals(record.Key.Namespace, cacheNamespace, StringComparison.Ordinal))
                continue;

            if (!string.Equals(record.Key.Key, probeKey, StringComparison.Ordinal))
                continue;

            found = true;
            if (record.Sequence >= lastSeq)
                lastSeq = record.Sequence;
        }

        return (found, lastSeq);
    }

    /// <summary>Returns the number of distinct keys with at least one Put in the journal tail.</summary>
    /// <param name="dataDir">Node persistence directory.</param>
    /// <param name="cacheNamespace">Cache namespace to count.</param>
    /// <param name="fromSegment">First journal segment index to scan.</param>
    /// <returns>Count of unique keys with Put operations.</returns>
    public static int CountUniquePutKeys(string dataDir, string cacheNamespace, int fromSegment = 1)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        using var records = JournalReadPath.ReadAll(dataDir, fromSegment, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is not JournalOperationKind.Put)
                continue;

            if (!string.Equals(record.Key.Namespace, cacheNamespace, StringComparison.Ordinal))
                continue;

            _ = keys.Add(record.Key.Key);
        }

        return keys.Count;
    }
}
