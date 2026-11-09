using System;
using System.Buffers;
using System.Buffers.Binary;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Serialization.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Encoding of replicated mutation outcomes: applied flag plus optional previous entry.</summary>
/// <remarks>
/// Layout is a one-byte applied flag, a 32-bit little-endian previous length, then the previous
/// entry bytes. An empty previous section means no previous value was observed.
/// </remarks>
internal static class ReplicaOutcomeCodec
{
    private const int HeaderLength = 1 + sizeof(int);
    private const byte AccessValuePresent = 1;

    internal static ReadOnlyMemory<byte> Applied { get; } = CreateBuffer(true, 0);

    internal static ReadOnlyMemory<byte> NotApplied { get; } = CreateBuffer(false, 0);

    /// <summary>Records the bytes observed by an access, distinguishing an empty value from a miss.</summary>
    /// <param name="applied">Whether a renewal changed the deadline.</param>
    /// <param name="payload">The value observed by Get, or null for a miss or Refresh.</param>
    /// <returns>The canonical outcome retained by the group.</returns>
    internal static ReadOnlyMemory<byte> EncodeAccess(bool applied, ReadOnlyMemory<byte>? payload)
    {
        if (payload is not { } value)
            return applied ? Applied : NotApplied;

        var bytes = CreateBuffer(applied, checked(1 + value.Length));
        bytes[HeaderLength] = AccessValuePresent;
        value.Span.CopyTo(bytes.AsSpan(HeaderLength + 1));
        return bytes;
    }

    /// <summary>Checks the optional value section of a distributed Get outcome.</summary>
    /// <param name="value">The retained value section, without the common outcome header.</param>
    /// <returns>Whether the section encodes a miss or a present value.</returns>
    internal static bool IsAccessValue(ReadOnlyMemory<byte> value) => value.IsEmpty || value.Span[0] == AccessValuePresent;

    /// <summary>Returns the originally observed value, including when a subsequent write changed the key.</summary>
    /// <param name="outcome">The canonical outcome.</param>
    /// <returns>The original payload, or null for a miss or Refresh.</returns>
    /// <exception cref="InvalidOperationException">The canonical outcome is malformed.</exception>
    internal static ReadOnlyMemory<byte>? DecodeAccess(ReadOnlyMemory<byte> outcome)
    {
        var valid = TryDecode(outcome, out _, out var value) && IsAccessValue(value);
        ReadOnlyMemory<byte>? payload = value.IsEmpty ? default(ReadOnlyMemory<byte>?) : value[1..];
        return valid ? payload : throw new InvalidOperationException("Committed distributed access outcome is malformed.");
    }

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
    internal static CacheRemoveResult<object?> DecodeRemove(ReadOnlyMemory<byte> outcome)
    {
        const string message = "Committed remove outcome payload is malformed.";
        if (!TryDecode(outcome, out var removed, out var previous) || (removed && previous.IsEmpty))
            throw new InvalidOperationException(message);

        if (!removed)
            return new CacheRemoveResult<object?>(false, null);

        var entry = CacheEntryWire.Parser.ParseFrom(new ReadOnlySequence<byte>(previous));
        try
        {
            var mapped = entry.MapFromProto<object?>();
            return new CacheRemoveResult<object?>(true, mapped.Value);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.InvalidArgument)
        {
            throw new InvalidOperationException(message, ex);
        }
    }

    /// <summary>Encodes an outcome to its canonical bytes.</summary>
    /// <param name="applied">Whether the mutation took effect.</param>
    /// <param name="previous">Previous entry bytes, or empty when none was observed.</param>
    /// <returns>The canonical outcome bytes.</returns>
    internal static byte[] Encode(bool applied, ReadOnlyMemory<byte> previous)
    {
        var bytes = CreateBuffer(applied, previous.Length);
        previous.Span.CopyTo(bytes.AsSpan(HeaderLength));
        return bytes;
    }

    /// <summary>Encodes a successful remove directly into its canonical outcome buffer.</summary>
    /// <param name="previous">The previous entry observed by the mutation.</param>
    /// <returns>The canonical outcome bytes, including the serialized previous entry.</returns>
    internal static byte[] EncodeRemove(CacheEntryWire previous)
    {
        var bytes = CreateBuffer(true, previous.CalculateSize());
        previous.WriteTo(bytes.AsSpan(HeaderLength));
        return bytes;
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
        if (span.Length < HeaderLength)
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
