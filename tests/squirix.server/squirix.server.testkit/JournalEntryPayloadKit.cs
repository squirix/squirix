using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.TestKit;

/// <summary>Helpers for building journal Put entry payloads in tests and benchmarks.</summary>
public static class JournalEntryPayloadKit
{
    /// <summary>Encodes a cache entry into an exact-size owned buffer for tests.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to encode.</param>
    /// <returns>Exact-size owned encoded bytes.</returns>
    public static byte[] Encode<T>(NodeCacheEntry<T> entry)
    {
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var buffer = JournalEntryPayload.Encode(in prepared);
        return FixtureBufferKit.CopyToOwned(buffer.Span);
    }

    /// <summary>Encodes a Put journal payload for the given cache value into an exact-size owned buffer.</summary>
    /// <param name="value">Cache value to encode.</param>
    /// <param name="version">Entry version.</param>
    /// <returns>Binary cache-entry bytes for a journal Put frame.</returns>
    public static byte[] EncodePut(object? value, long version = 1) => Encode(new NodeCacheEntry<object?> { Value = value, Version = version });
}
