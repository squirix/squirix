using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Core;
using Squirix.Transport.Binary;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Version-one peer RPC framing over mutually authenticated TLS. One in-flight call owns a stream.</summary>
internal static class PeerFrames
{
    internal const int RequestBytes = 24;
    internal const int ReplyBytes = 12;
    internal static readonly SslApplicationProtocol Protocol = new("squirix-peer/1");
    internal static readonly UTF8Encoding Text = new(false, true);
    private const uint Magic = 0x53515031;

    internal static void WriteRequest(Span<byte> header, in Request request)
    {
        BinaryPrimitives.WriteUInt32BigEndian(header, Magic);
        BinaryPrimitives.WriteInt32BigEndian(header[4..], request.Body);
        BinaryPrimitives.WriteInt64BigEndian(header[8..], request.Budget);
        BinaryPrimitives.WriteUInt16BigEndian(header[16..], Convert.ToUInt16(request.Path));
        BinaryPrimitives.WriteUInt16BigEndian(header[18..], Convert.ToUInt16(request.Fingerprint));
        BinaryPrimitives.WriteUInt16BigEndian(header[20..], Convert.ToUInt16(request.Parent));
        BinaryPrimitives.WriteUInt16BigEndian(header[22..], Convert.ToUInt16(request.State));
    }

    internal static Request ReadRequest(ReadOnlySpan<byte> header)
    {
        CheckMagic(header);
        var request = new Request(
            BinaryPrimitives.ReadInt32BigEndian(header[4..]),
            BinaryPrimitives.ReadInt64BigEndian(header[8..]),
            BinaryPrimitives.ReadUInt16BigEndian(header[16..]),
            BinaryPrimitives.ReadUInt16BigEndian(header[18..]),
            BinaryPrimitives.ReadUInt16BigEndian(header[20..]),
            BinaryPrimitives.ReadUInt16BigEndian(header[22..]));
        var invalid = request.Body is < 0 or > EntryLimits.MaxReceiveMessageSizeBytes || request.Path is 0 or > 96 ||
            request.Fingerprint > 128 || request.Parent > Frames.MaxTraceParentLength || request.State > Frames.MaxTraceStateLength ||
            request.Budget is < -1 or 0 || request.Budget > (uint.MaxValue - 1L) * TimeSpan.TicksPerMillisecond;
        return invalid ? throw new InvalidDataException("Invalid peer frame dimensions or deadline.") : request;
    }

    internal static void WriteReply(Span<byte> header, int status, int length)
    {
        BinaryPrimitives.WriteUInt32BigEndian(header, Magic);
        BinaryPrimitives.WriteInt32BigEndian(header[4..], status);
        BinaryPrimitives.WriteInt32BigEndian(header[8..], length);
    }

    internal static (int Status, int Size) ReadReply(ReadOnlySpan<byte> header)
    {
        CheckMagic(header);
        var status = BinaryPrimitives.ReadInt32BigEndian(header[4..]);
        var size = BinaryPrimitives.ReadInt32BigEndian(header[8..]);
        return status is < 0 or > 16 || size is < 0 or > EntryLimits.MaxReceiveMessageSizeBytes
            ? throw new InvalidDataException("Invalid peer response status or length.")
            : (status, size);
    }

    private static void CheckMagic(ReadOnlySpan<byte> header)
    {
        if (BinaryPrimitives.ReadUInt32BigEndian(header) != Magic)
            throw new InvalidDataException("Unsupported Squirix peer protocol version.");
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Request(int Body, long Budget, int Path, int Fingerprint, int Parent, int State)
    {
        internal int Metadata => Path + Fingerprint + Parent + State;
    }
}
