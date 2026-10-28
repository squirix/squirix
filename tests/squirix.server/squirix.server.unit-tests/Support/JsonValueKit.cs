using System;
using System.Buffers.Binary;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Builds JSON cache values and the cache-entry bytes around them for codec tests.</summary>
internal static class JsonValueKit
{
    /// <summary>The value kind byte of a JSON value in an encoded entry.</summary>
    internal const byte JsonKind = 7;

    private const int EnvelopeLength = 1 + 1 + 8 + 2;
    private const int VersionOffset = 2;

    /// <summary>Creates a value over the UTF-8 bytes of valid JSON text.</summary>
    /// <param name="json">The JSON text.</param>
    /// <returns>The value.</returns>
    /// <exception cref="ArgumentException">The text is not one JSON value.</exception>
    internal static JsonUtf8Value Create(string json) =>
        JsonUtf8Value.TryCreate(Encoding.UTF8.GetBytes(json), out var value) ? value : throw new ArgumentException("The text is not one JSON value.", nameof(json));

    /// <summary>Encodes an entry with the production codec.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns>The entry bytes.</returns>
    internal static byte[] Encode(NodeCacheEntry<object?> entry)
    {
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);
        return encoded;
    }

    /// <summary>Encodes an entry that holds the bytes as a JSON value, without checking them.</summary>
    /// <param name="json">The bytes to place after the JSON kind and its length.</param>
    /// <returns>The entry bytes: no deadline, version 1, no tags.</returns>
    internal static byte[] EncodeEntryAround(ReadOnlySpan<byte> json)
    {
        var encoded = new byte[EnvelopeLength + 1 + sizeof(uint) + json.Length];
        BinaryPrimitives.WriteInt64LittleEndian(encoded.AsSpan(VersionOffset), 1);
        encoded[EnvelopeLength] = JsonKind;
        BinaryPrimitives.WriteUInt32LittleEndian(encoded.AsSpan(EnvelopeLength + 1), uint.CreateTruncating(json.Length));
        json.CopyTo(encoded.AsSpan(EnvelopeLength + 1 + sizeof(uint)));
        return encoded;
    }
}
