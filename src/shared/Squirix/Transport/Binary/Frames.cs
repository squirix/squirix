using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Transport.Binary;

/// <summary>Encodes the versioned binary cache request and reply headers.</summary>
internal static class Frames
{
    /// <summary>Identifies the versioned binary cache protocol.</summary>
    internal const uint Preface = 0x53514233; // SQB3
    internal const byte Accepted = 1;
    internal const byte Rejected = 2;
    internal const byte AcceptedWithMutationReplay = 3;
    internal const byte ExtendedMode = 2;
    internal const string OwnerUnreachableDetail = "owner_unreachable";
    internal const string ErrorCodeTrailer = "squirix-error-code";
    internal const string PayloadTooLargeCode = "PAYLOAD_TOO_LARGE";
    internal const int PrefaceBytes = 5;
    internal const int RequestBytes = 10;
    internal const int ReplyBytes = 8;

    /// <summary>The uppermost control bit marks a trace envelope. The low 16 bits carry the timeout.</summary>
    internal const int TracedRequest = int.MinValue;
    internal const int MaxTraceParentLength = 256;
    internal const int MaxTraceStateLength = 512;
    internal const int MaxRequestBytes = 8 * 1024 * 1024;
    internal const int MaxResponseBytes = 8 * 1024 * 1024;
    internal const int MaxValueBytes = 512 * 1024 * 1024;
    internal const int MaxEnvelopeBytes = 1024 * 1024;
    internal const int MaxExtendedBodyBytes = MaxValueBytes + MaxEnvelopeBytes;
    internal const int ExtendedSize = 0x00ff_ffff;
    internal const int ExtendedRequestBytes = 2 * sizeof(int);
    internal const int ExtendedReplyBytes = sizeof(int);
    internal const int MaxBatchBytes = 16 * 1024;
    internal const int MaxBatch = 16;

    private const int SizeMask = 0x00ff_ffff;
    private const int OperationMask = 0x007f_0000;

    internal static int ReadOperationId(int control) => (control & OperationMask) >>> 16;

    internal static (int Id, int Size, int Control) ReadRequest(ReadOnlySpan<byte> bytes)
    {
        var word = BinaryPrimitives.ReadInt64BigEndian(bytes);
        var id = int.CreateTruncating(word >> 32);
        var fields = int.CreateTruncating(word);
        var size = fields & SizeMask;
        var timeout = BinaryPrimitives.ReadUInt16BigEndian(bytes[8..]);
        var control = (fields & TracedRequest) | ((fields >>> 8) & OperationMask) | timeout;
        return id >= 0 && (size <= MaxRequestBytes || size == ExtendedSize) ? (id, size, control) : ThrowInvalidFrame();
    }

    internal static (int Id, int Size, int Status) ReadReply(ReadOnlySpan<byte> bytes)
    {
        var word = BinaryPrimitives.ReadInt64BigEndian(bytes);
        var id = int.CreateTruncating(word >> 32);
        var fields = int.CreateTruncating(word);
        var size = fields & SizeMask;
        var status = fields >>> 24;
        return id >= 0 && (size <= MaxResponseBytes || size == ExtendedSize) && status <= 16 ? (id, size, status) : ThrowInvalidFrame();
    }

    internal static void WriteRequest(Span<byte> bytes, int id, int size, int control)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        if (size != ExtendedSize)
            ArgumentOutOfRangeException.ThrowIfGreaterThan(size, MaxRequestBytes);
        if ((control & 0x7f80_0000) != 0)
            throw new ArgumentOutOfRangeException(nameof(control));

        var fields = (control & TracedRequest) | ((control & OperationMask) << 8) | size;
        BinaryPrimitives.WriteInt64BigEndian(bytes, (long.CreateChecked(id) << 32) | uint.CreateTruncating(fields));
        WriteRequestTimeout(bytes, ushort.CreateTruncating(control));
    }

    internal static void WriteRequestTimeout(Span<byte> bytes, ushort timeout) => BinaryPrimitives.WriteUInt16BigEndian(bytes[8..], timeout);

    internal static void WriteReply(Span<byte> bytes, int id, int size, int status)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        if (size != ExtendedSize)
            ArgumentOutOfRangeException.ThrowIfGreaterThan(size, MaxResponseBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(status);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(status, 16);
        BinaryPrimitives.WriteInt64BigEndian(bytes, (long.CreateChecked(id) << 32) | uint.CreateTruncating((status << 24) | size));
    }

    internal static void WriteRequestMessage(Span<byte> frame, int id, int size, int control, WireMessage message)
    {
        WriteRequest(frame, id, size, control);
        message.WriteTo(frame.Slice(RequestBytes, size));
    }

    internal static void WriteReplyMessage(Span<byte> frame, int id, int size, int status, WireMessage message)
    {
        WriteReply(frame, id, size, status);
        message.WriteTo(frame.Slice(ReplyBytes, size));
    }

    internal static int ReadExtendedSize(ReadOnlySpan<byte> bytes)
    {
        var size = BinaryPrimitives.ReadInt32BigEndian(bytes);
        return size is > MaxRequestBytes and <= MaxExtendedBodyBytes ? size : throw new InvalidDataException("Invalid extended cache frame length.");
    }

    internal static void WriteExtendedSize(Span<byte> bytes, int size) => BinaryPrimitives.WriteInt32BigEndian(bytes, size);

    private static (int Id, int Size, int Control) ThrowInvalidFrame() => throw new InvalidDataException("Invalid binary cache frame.");
}
