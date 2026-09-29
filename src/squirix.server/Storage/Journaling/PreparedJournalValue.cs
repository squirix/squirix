using System;
using System.Buffers;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Storage.Journaling;

/// <summary>A cache value normalized and encoded once, so an entry around it is assembled without serializing the value again.</summary>
/// <remarks>Dispose returns the pooled buffer; callers must not retain <see cref="Memory" /> afterwards.</remarks>
[Immutable]
internal sealed class PreparedJournalValue : IDisposable
{
    private readonly byte[] _buffer;
    private int _disposed;

    private PreparedJournalValue(byte[] buffer, int encodedLength)
    {
        _buffer = buffer;
        EncodedLength = encodedLength;
    }

    /// <summary>Gets the encoded length of the value.</summary>
    internal int EncodedLength { get; }

    /// <summary>Gets the encoded value bytes.</summary>
    internal ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, EncodedLength);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        ArrayPool<byte>.Shared.ReturnCleared(_buffer);
    }

    /// <summary>Encodes an already normalized value, after checking its size, so an oversized value is rejected before it is copied.</summary>
    /// <param name="normalizedValue">The normalized value.</param>
    /// <returns>The prepared value.</returns>
    /// <exception cref="Squirix.Server.Errors.SquirixException">The value alone exceeds the entry size limit.</exception>
    internal static PreparedJournalValue Create(object? normalizedValue)
    {
        var length = CacheEntryCodec.ComputeValueLength(normalizedValue);
        EntryPayloadSizeGuard.EnsureLengthWithinLimit(length);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            CacheEntryCodec.WriteValue(normalizedValue, buffer);
            return new PreparedJournalValue(buffer, length);
        }
        catch
        {
            ArrayPool<byte>.Shared.ReturnCleared(buffer);
            throw;
        }
    }
}
