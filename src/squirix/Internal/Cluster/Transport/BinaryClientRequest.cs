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
[Immutable]
internal readonly record struct BinaryClientRequest(int Control, WireMessage Message, int Size, string? MutationOperationId)
{
    internal void WriteTo(Span<byte> frame, int id, int timeout = 0)
    {
        Frames.WriteRequest(frame, id, Size, Control | timeout);
        Message.WriteTo(frame.Slice(Frames.RequestBytes, Size), MutationOperationId);
    }
}
