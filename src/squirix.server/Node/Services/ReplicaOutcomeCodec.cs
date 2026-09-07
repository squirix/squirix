using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;

namespace Squirix.Server.Node.Services;

/// <summary>Encoding of replicated mutation outcomes: applied flag plus optional previous entry.</summary>
/// <remarks>
/// Layout is a one-byte applied flag, a 32-bit little-endian previous length, then the previous
/// entry bytes. An empty previous section means no previous value was observed.
/// </remarks>
internal static class ReplicaOutcomeCodec
{
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
