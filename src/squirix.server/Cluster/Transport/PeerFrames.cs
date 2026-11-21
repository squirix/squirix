using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Core;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Version-two peer RPC framing over mutually authenticated TLS. One in-flight call owns a stream.</summary>
internal static class PeerFrames
{
    internal const int RequestBytes = 20;
    internal const int ReplyBytes = 8;
    internal static readonly TimeSpan IdleTimeout = RpcAttemptTimeout.PerAttempt;
    internal static readonly SslApplicationProtocol Protocol = new("squirix-peer/2");
    internal static readonly UTF8Encoding Text = new(false, true);
    private const uint Magic = 0x53515032;
    private const int SizeMask = 0x00ff_ffff;
    private const int DigestFingerprintFlag = 1 << 27;
    private const int BinaryTraceParentFlag = 1 << 28;
    private static readonly SearchValues<char> UppercaseHex = SearchValues.Create("0123456789ABCDEF");

    internal static (byte[] Bytes, bool IsDigest) EncodeFingerprint(string? fingerprint)
    {
        if (string.IsNullOrEmpty(fingerprint))
            return ([], false);

        return fingerprint.Length == RingFingerprint.DigestBytes * 2 && !fingerprint.AsSpan().ContainsAnyExcept(UppercaseHex)
            ? (Convert.FromHexString(fingerprint), true)
            : (Text.GetBytes(fingerprint), false);
    }

    internal static void WriteRequest(Span<byte> header, in Request request)
    {
        if (request.Body is < 0 or > EntryLimits.MaxSendMessageSizeBytes ||
            request.Fingerprint is < 0 or > 128 || (request.FingerprintIsDigest && request.Fingerprint != RingFingerprint.DigestBytes) ||
            request.Parent is < 0 or > Frames.MaxTraceParentLength || (request.TraceParentIsBinary && request.Parent != WireTraceParent.ByteCount) ||
            request.State is < 0 or > Frames.MaxTraceStateLength)
            throw new ArgumentOutOfRangeException(nameof(request));

        var operation = uint.CreateChecked(Unsafe.BitCast<PeerOperation, byte>(request.Operation));
        var body = (operation << 24) | uint.CreateChecked(request.Body);
        var flags = (request.FingerprintIsDigest ? DigestFingerprintFlag : 0) | (request.TraceParentIsBinary ? BinaryTraceParentFlag : 0);
        var metadata = flags | (request.Fingerprint << 19) | (request.Parent << 10) | request.State;
        BinaryPrimitives.WriteUInt64BigEndian(header, (ulong.CreateChecked(Magic) << 32) | body);
        BinaryPrimitives.WriteInt64BigEndian(header[8..], request.Budget);
        BinaryPrimitives.WriteInt32BigEndian(header[16..], metadata);
    }

    internal static Request ReadRequest(ReadOnlySpan<byte> header)
    {
        var body = ReadPackedWord(header);
        var metadata = BinaryPrimitives.ReadInt32BigEndian(header[16..]);
        var request = new Request(
            body & SizeMask,
            BinaryPrimitives.ReadInt64BigEndian(header[8..]),
            Unsafe.BitCast<byte, PeerOperation>(byte.CreateTruncating(body >>> 24)),
            (metadata >>> 19) & 0xff,
            (metadata >>> 10) & 0x1ff,
            metadata & 0x3ff,
            (metadata & DigestFingerprintFlag) != 0,
            (metadata & BinaryTraceParentFlag) != 0);
        var invalid = request.Body > EntryLimits.MaxReceiveMessageSizeBytes || (metadata >>> 29) != 0 ||
            (request.TraceParentIsBinary && request.Parent != WireTraceParent.ByteCount) ||
            (request.FingerprintIsDigest && request.Fingerprint != RingFingerprint.DigestBytes) ||
            request.Fingerprint > 128 || request.Parent > Frames.MaxTraceParentLength || request.State > Frames.MaxTraceStateLength ||
            request.Budget is < -1 or 0 || request.Budget > (uint.MaxValue - 1L) * TimeSpan.TicksPerMillisecond;
        return invalid ? throw new InvalidDataException("Invalid peer frame dimensions or deadline.") : request;
    }

    internal static void WriteReply(Span<byte> header, int status, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(status);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(status, 16);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, EntryLimits.MaxSendMessageSizeBytes);
        BinaryPrimitives.WriteUInt64BigEndian(header, (ulong.CreateChecked(Magic) << 32) | uint.CreateTruncating((status << 24) | length));
    }

    internal static (int Status, int Size) ReadReply(ReadOnlySpan<byte> header)
    {
        var word = ReadPackedWord(header);
        var status = word >>> 24;
        var size = word & SizeMask;
        return status > 16 || size > EntryLimits.MaxReceiveMessageSizeBytes
            ? throw new InvalidDataException("Invalid peer response status or length.")
            : (status, size);
    }

    private static int ReadPackedWord(ReadOnlySpan<byte> header)
    {
        var word = BinaryPrimitives.ReadUInt64BigEndian(header);
        return word >> 32 == Magic
            ? int.CreateTruncating(word)
            : throw new InvalidDataException("Unsupported Squirix peer protocol version.");
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Request(int Body, long Budget, PeerOperation Operation, int Fingerprint, int Parent, int State, bool FingerprintIsDigest = false, bool TraceParentIsBinary = false)
    {
        internal int Metadata => Fingerprint + Parent + State;
    }
}
