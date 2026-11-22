using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>A normalized cache value prepared for entry assembly without repeated serialization.</summary>
/// <remarks>Immutable JSON is retained directly; other values own a pooled encoding until disposal.</remarks>
[Immutable]
internal sealed class PreparedJournalValue : IDisposable
{
    private readonly byte[]? _buffer;
    private readonly JsonUtf8Value? _json;
    private int _disposed;

    private PreparedJournalValue(byte[]? buffer, JsonUtf8Value? json, int encodedLength)
    {
        _buffer = buffer;
        _json = json;
        EncodedLength = encodedLength;
    }

    /// <summary>Gets the encoded length of the value.</summary>
    internal int EncodedLength { get; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (_buffer is { } buffer)
            PayloadBuffer.Release(buffer);
    }

    /// <summary>Retains immutable JSON or snapshots another normalized value into a pooled encoding.</summary>
    /// <param name="normalizedValue">The normalized value.</param>
    /// <returns>The prepared value.</returns>
    internal static PreparedJournalValue Create(object? normalizedValue)
    {
        var length = CacheEntryCodec.ComputeValueLength(normalizedValue);
        if (normalizedValue is JsonUtf8Value json)
            return new PreparedJournalValue(null, json, length);

        var buffer = PayloadBuffer.Rent(length);
        try
        {
            CacheEntryCodec.WriteValue(normalizedValue, buffer);
            return new PreparedJournalValue(buffer, null, length);
        }
        catch
        {
            PayloadBuffer.Release(buffer);
            throw;
        }
    }

    /// <summary>Writes the value directly into its final entry, without a temporary encoding for immutable JSON.</summary>
    /// <param name="destination">At least <see cref="EncodedLength" /> bytes reserved for the encoded value.</param>
    internal void WriteTo(Span<byte> destination)
    {
        if (_json is { } json)
            CacheEntryCodec.WriteValue(json, destination);
        else
            _buffer.AsSpan(0, EncodedLength).CopyTo(destination);
    }
}
