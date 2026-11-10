using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.TestKit;

/// <summary>Helpers for constructing entry payloads near fixed size limits in tests.</summary>
public static class EntryLimitKit
{
    private static readonly Dictionary<int, string> AtMostCache = [];
    private static readonly Lock AtMostGate = new();
    private static readonly Lazy<string> OverEntryLimit = new(static () => new string('x', FindLongestFittingLength(EntryLimits.MaxEntrySizeBytes) + 1));

    /// <summary>Returns the largest string payload whose encoded entry bytes fit the fixed server entry limit.</summary>
    /// <returns>A near-limit string value for benchmarks and integration tests.</returns>
    public static string CreateNearLimitStringValue() => CreateStringAtMostSerializedBytes(EntryLimits.MaxEntrySizeBytes);

    /// <summary>Returns the largest string payload whose encoded entry size is at most <paramref name="maxSerializedBytes" />.</summary>
    /// <param name="maxSerializedBytes">Maximum allowed entry byte length.</param>
    /// <returns>A string value whose serialized entry size is within the limit. The value is shared between calls with the same limit.</returns>
    public static string CreateStringAtMostSerializedBytes(int maxSerializedBytes)
    {
        lock (AtMostGate)
        {
            if (AtMostCache.TryGetValue(maxSerializedBytes, out var cached))
                return cached;

            var value = new string('x', FindLongestFittingLength(maxSerializedBytes));
            AtMostCache.Add(maxSerializedBytes, value);
            return value;
        }
    }

    /// <summary>Returns the smallest string payload whose encoded entry exceeds <see cref="EntryLimits.MaxEntrySizeBytes" />.</summary>
    /// <returns>A string value guaranteed to exceed the entry limit once serialized. The value is shared between calls.</returns>
    public static string CreateStringOverEntryLimit() => OverEntryLimit.Value;

    /// <summary>Finds the longest string length that fits; a string value encodes as a fixed header plus one byte per ASCII character, so two small measurements suffice.</summary>
    /// <param name="maxSerializedBytes">Maximum allowed entry byte length.</param>
    /// <returns>The longest string length whose entry fits the limit.</returns>
    /// <exception cref="InvalidOperationException">The string encoding is no longer one byte per ASCII character.</exception>
    private static int FindLongestFittingLength(int maxSerializedBytes)
    {
        var overhead = MeasureStringPayload(0);
        var perCharacter = MeasureStringPayload(1) - overhead;
        return perCharacter == 1
            ? Math.Max(0, maxSerializedBytes - overhead)
            : throw new InvalidOperationException("The string entry encoding is no longer one byte per ASCII character; update EntryLimitKit.");
    }

    private static int MeasureStringPayload(int stringLength) =>
        JournalEntryPayload.MeasureSerializedBytes(new NodeCacheEntry<object?> { Value = new string('x', stringLength), Version = 1 });
}
