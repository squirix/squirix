using System;
using Squirix.Attributes;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Holds the frame size and mutation identity captured for one transport attempt.</summary>
/// <param name="Control">Operation and trace bits for the frame header.</param>
/// <param name="Message">Request body, including any trace envelope.</param>
/// <param name="Size">Encoded body size.</param>
/// <param name="MutationOperationId">Identity to write for this attempt, empty when omitted.</param>
/// <param name="ValueBytes">Serialized value length declared by an extended write.</param>
[Immutable]
internal readonly record struct BinaryClientRequest(int Control, WireMessage Message, int Size, string? MutationOperationId, int ValueBytes = 0)
{
    internal int HeaderBytes => Frames.RequestBytes + (Size > Frames.MaxRequestBytes ? Frames.ExtendedRequestBytes : 0);

    internal void WriteTo(Span<byte> frame, int id, int timeout = 0)
    {
        WriteHeader(frame, id, timeout);
        Message.WriteTo(frame.Slice(HeaderBytes, Size), MutationOperationId);
    }

    internal void WriteHeader(Span<byte> frame, int id, int timeout = 0)
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
}
