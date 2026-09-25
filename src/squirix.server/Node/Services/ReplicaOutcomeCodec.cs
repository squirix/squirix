using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Encoding of replicated mutation outcomes: applied flag plus optional previous entry.</summary>
/// <remarks>
/// Layout is a one-byte applied flag, a 32-bit little-endian previous length, then the previous
/// entry bytes. An empty previous section means no previous value was observed.
/// </remarks>
internal static class ReplicaOutcomeCodec
{
    /// <summary>Decodes a committed outcome to its applied flag.</summary>
    /// <param name="outcome">The committed outcome payload.</param>
    /// <returns><see langword="true" /> when the mutation took effect.</returns>
    /// <exception cref="InvalidOperationException">The payload is malformed.</exception>
    internal static bool DecodeApplied(ReadOnlyMemory<byte> outcome)
    {
        const string message = "Committed outcome payload is malformed.";
        return !TryDecode(outcome, out var applied, out _) ? throw new InvalidOperationException(message) : applied;
    }

    /// <summary>Decodes a committed remove outcome to the removed entry, if any.</summary>
    /// <param name="outcome">The committed outcome payload.</param>
    /// <returns>The remove outcome with the previous value when one was observed.</returns>
    /// <exception cref="InvalidOperationException">The payload is malformed.</exception>
    internal static async Task<CacheRemoveResult<object?>> DecodeRemoveAsync(ReadOnlyMemory<byte> outcome)
    {
        if (!TryDecode(outcome, out var removed, out var previous) || (removed && previous.IsEmpty))
            throw new InvalidOperationException("Committed remove outcome payload is malformed.");

        if (!removed)
            return new CacheRemoveResult<object?>(false, null);

        var entry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(previous));
        var mapped = await entry.MapFromProtoAsync<object?>().ConfigureAwait(false);
        return new CacheRemoveResult<object?>(true, mapped.Value);
    }

    /// <summary>Encodes an outcome to its canonical bytes.</summary>
    /// <param name="applied">Whether the mutation took effect.</param>
    /// <param name="previous">Previous entry bytes, or empty when none was observed.</param>
    /// <returns>The canonical outcome bytes.</returns>
    [SuppressMessage("Usage", "MA0045:Use async disposable", Justification = "BinaryWriter and MemoryStream are in-memory and are intentionally encoded synchronously before durable asynchronous I/O.")]
    internal static byte[] Encode(bool applied, ReadOnlyMemory<byte> previous)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
        {
            writer.Write(applied);
            writer.Write(previous.Length);
            if (!previous.IsEmpty)
                writer.Write(previous.Span);
        }

        return stream.ToArray();
    }

    /// <summary>Decodes canonical outcome bytes.</summary>
    /// <param name="bytes">The canonical outcome bytes.</param>
    /// <param name="applied">Whether the mutation took effect.</param>
    /// <param name="previous">Previous entry bytes, possibly empty.</param>
    /// <returns><see langword="true" /> when the payload is structurally valid; otherwise <see langword="false" />.</returns>
    internal static bool TryDecode(ReadOnlyMemory<byte> bytes, out bool applied, out ReadOnlyMemory<byte> previous)
    {
        applied = false;
        previous = default;
        var span = bytes.Span;
        if (span.Length < 5)
            return false;

        applied = span[0] != 0;
        var previousLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, 4));
        if (previousLength < 0 || previousLength != span.Length - 5)
            return false;

        previous = bytes.Slice(5, previousLength);
        return true;
    }
}
