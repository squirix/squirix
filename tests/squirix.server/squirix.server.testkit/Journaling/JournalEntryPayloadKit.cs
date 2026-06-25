using Squirix.Server.Storage.Journaling.Entries;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Helpers for building journal Put entry payloads in tests and benchmarks.</summary>
public static class JournalEntryPayloadKit
{
    /// <summary>Encodes a Put journal payload for the given cache value.</summary>
    /// <param name="value">Cache value to encode.</param>
    /// <param name="version">Entry version.</param>
    /// <returns>Binary cache-entry bytes for a journal Put frame.</returns>
    public static byte[] EncodePut(object? value, long version = 1) =>
        JournalEntryPayload.Encode(new CacheEntry<object?> { Value = value, Version = version });
}
