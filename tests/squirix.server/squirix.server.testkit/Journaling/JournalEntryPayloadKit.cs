using System;
using System.Buffers;
using Squirix.Server.Storage.Journaling.Entries;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Helpers for building journal Put entry payloads in tests and benchmarks.</summary>
public static class JournalEntryPayloadKit
{
    /// <summary>Encodes a Put journal payload for the given cache value into an exact-size owned buffer.</summary>
    /// <param name="value">Cache value to encode.</param>
    /// <param name="version">Entry version.</param>
    /// <returns>Binary cache-entry bytes for a journal Put frame.</returns>
    public static byte[] EncodePut(object? value, long version = 1) =>
        Encode(new NodeCacheEntry<object?> { Value = value, Version = version });

    /// <summary>Encodes a cache entry into an exact-size owned buffer for tests.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="entry">The entry to encode.</param>
    /// <returns>Exact-size owned encoded bytes.</returns>
    private static byte[] Encode<T>(NodeCacheEntry<T> entry)
    {
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        var length = JournalEntryPayload.Encode(in prepared, out var buffer);
        try
        {
            // Exact-size owned buffer for test fixtures that retain PutEntryBytes beyond the encode call.
#pragma warning disable ZA0302
            var owned = new byte[length];
#pragma warning restore ZA0302
            buffer.AsSpan(0, length).CopyTo(owned);
            return owned;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
