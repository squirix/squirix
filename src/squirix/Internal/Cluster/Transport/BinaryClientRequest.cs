using System;
using Squirix.Attributes;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Holds the frame size and mutation identity captured for one transport attempt.</summary>
/// <param name="Control">Operation and trace bits for the frame header.</param>
/// <param name="Message">Request body, including any trace envelope.</param>
/// <param name="Size">Encoded body size.</param>
/// <param name="MutationOperationId">Identity to write for this attempt, empty when omitted.</param>
/// <param name="ValueBytes">Serialized value length declared by an extended write.</param>
/// <param name="HasBorrowedBytes">Whether the encoder can send existing value bytes directly.</param>
/// <param name="Encoded">A captured inline message body when preparation encoded it in full.</param>
[Immutable]
internal readonly record struct BinaryClientRequest(int Control, WireMessage Message, int Size, string? MutationOperationId, int ValueBytes, bool HasBorrowedBytes, byte[]? Encoded = null)
{
    internal int HeaderBytes => Frames.RequestBytes + (Size > Frames.MaxRequestBytes ? Frames.ExtendedRequestBytes : 0);

    internal BinaryClientRequest Measure(bool supportsExtendedFrames, Span<byte> destination, byte[]? capture = null)
    {
        var bodySize = !destination.IsEmpty
            ? Message.WriteOrMeasure(destination, MutationOperationId, out var hasBorrowedBytes)
            : Message.CalculateSize(MutationOperationId, out hasBorrowedBytes);
        var frameLimit = supportsExtendedFrames ? Frames.MaxExtendedBodyBytes : Frames.MaxRequestBytes;
        if (bodySize > frameLimit)
            throw RequestTooLarge();

        var original = Message is BinaryTracedRequest { PayloadMessage: { } nested } ? nested : Message;
        var valueSize = bodySize <= Frames.MaxRequestBytes ? 0 : CacheWritePayload.Length(original);
        var encoded = capture != null && bodySize <= capture.Length ? capture : null;
        return valueSize is { } size
            ? this with { Size = bodySize, ValueBytes = size, HasBorrowedBytes = hasBorrowedBytes, Encoded = encoded }
            : throw RequestTooLarge();
    }

    internal void WriteTo(Span<byte> frame, int id, int timeout = 0)
    {
        WriteHeader(frame, id, timeout);
        var body = frame.Slice(HeaderBytes, Size);
        if (Encoded != null)
            Encoded.AsSpan(0, Size).CopyTo(body);
        else
            Message.WriteTo(body, MutationOperationId);
    }

    internal Memory<byte> WriteTo(WireSegments segments, int id, int timeout = 0)
    {
        if (!HasBorrowedBytes && Size <= Frames.MaxRequestBytes)
        {
            var frame = segments.ReserveFrame(HeaderBytes + Size);
            WriteTo(frame.Span, id, timeout);
            return frame[..HeaderBytes];
        }

        var header = segments.GetMemory(HeaderBytes)[..HeaderBytes];
        WriteHeader(header.Span, id, timeout);
        segments.Advance(header.Length);
        Message.WriteTo(segments, MutationOperationId);
        return header;
    }

    internal void WriteHeader(Span<byte> frame, int id, int timeout)
    {
        if (Size > Frames.MaxRequestBytes)
        {
            Frames.WriteRequest(frame, id, Frames.ExtendedSize, Control | timeout);
            Frames.WriteExtendedSize(frame[Frames.RequestBytes..], Size);
            Frames.WriteExtendedSize(frame[(Frames.RequestBytes + sizeof(int))..], ValueBytes);
        }
        else
        {
            Frames.WriteRequest(frame, id, Size, Control | timeout);
        }
    }

    private static SquirixTransportException RequestTooLarge() =>
        new(new CallStatus(CacheTransportStatusCode.ResourceExhausted, "Binary cache request is too large.", PayloadTooLarge: true));
}
