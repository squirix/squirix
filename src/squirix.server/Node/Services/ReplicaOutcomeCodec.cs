using System;
using System.Buffers.Binary;

namespace Squirix.Server.Node.Services;

/// <summary>Encoding of fixed-size replicated boolean outcomes.</summary>
/// <remarks>
/// The five canonical bytes contain a zero-or-one applied flag and a zero 32-bit value length.
/// Payload-bearing outcomes and noncanonical flags are rejected. Response bytes belong only to active reads.
/// </remarks>
internal static class ReplicaOutcomeCodec
{
    private const int HeaderLength = 1 + sizeof(int);

    internal static ReadOnlyMemory<byte> Applied { get; } = CreateBuffer(true, 0);

    internal static ReadOnlyMemory<byte> NotApplied { get; } = CreateBuffer(false, 0);

    /// <summary>Decodes a committed outcome to its applied flag.</summary>
    /// <param name="outcome">The committed outcome payload.</param>
    /// <returns><see langword="true" /> when the mutation took effect.</returns>
    /// <exception cref="InvalidOperationException">The payload is malformed.</exception>
    internal static bool DecodeApplied(ReadOnlyMemory<byte> outcome)
    {
        const string message = "Committed outcome payload is malformed.";
        return !TryDecode(outcome, out var applied, out var previous) || !previous.IsEmpty ? throw new InvalidOperationException(message) : applied;
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
        if (span.Length != HeaderLength || span[0] > 1)
            return false;

        applied = span[0] != 0;
        var previousLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, 4));
        if (previousLength < 0 || previousLength != span.Length - HeaderLength)
            return false;

        previous = bytes.Slice(HeaderLength, previousLength);
        return true;
    }

    private static byte[] CreateBuffer(bool applied, int previousLength)
    {
        var bytes = GC.AllocateUninitializedArray<byte>(checked(HeaderLength + previousLength));
        bytes[0] = Convert.ToByte(applied);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(1, sizeof(int)), previousLength);
        return bytes;
    }
}
