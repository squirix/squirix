using System;
using System.Buffers.Binary;
using System.IO;
using Google.Protobuf;

namespace Squirix.Transport.Binary;

/// <summary>Encodes the binary cache header. Requests pack an operation and timeout in the third word. Replies carry status.</summary>
internal static class Frames
{
    /// <summary>Identifies the versioned binary cache protocol.</summary>
    internal const uint Preface = 0x53514232; // SQB2
    internal const byte Accepted = 1;
    internal const byte Rejected = 2;
    internal const byte AcceptedWithMutationReplay = 3;
    internal const string OwnerUnreachableDetail = "owner_unreachable";
    internal const int PrefaceBytes = 5;
    internal const int HeaderBytes = 12;

    /// <summary>The uppermost control bit marks a trace envelope. The low 16 bits carry the timeout.</summary>
    internal const int TracedRequest = int.MinValue;
    internal const int MaxTraceParentLength = 256;
    internal const int MaxTraceStateLength = 512;
    internal const int MaxRequestBytes = 8 * 1024 * 1024;
    internal const int MaxResponseBytes = 8 * 1024 * 1024;
    internal const int MaxBatchBytes = 16 * 1024;
    internal const int MaxBatch = 16;

    internal static int ReadOperationId(int control) => (control & ~TracedRequest) >>> 16;

    internal static (int Id, int Size, int Control) Read(ReadOnlySpan<byte> bytes, int maxSize)
    {
        var id = BinaryPrimitives.ReadInt32BigEndian(bytes);
        var size = BinaryPrimitives.ReadInt32BigEndian(bytes[4..]);
        var control = BinaryPrimitives.ReadInt32BigEndian(bytes[8..]);
        return id >= 0 && size >= 0 && size <= maxSize ? (id, size, control) : ThrowInvalidFrame();
    }

    internal static void Write(Span<byte> bytes, int id, int size, int control)
    {
        BinaryPrimitives.WriteInt32BigEndian(bytes, id);
        BinaryPrimitives.WriteInt32BigEndian(bytes[4..], size);
        BinaryPrimitives.WriteInt32BigEndian(bytes[8..], control);
    }

    internal static void WriteMessage(Span<byte> frame, int id, int size, int control, IMessage message)
    {
        Write(frame, id, size, control);
        message.WriteTo(frame.Slice(HeaderBytes, size));
    }

    private static (int Id, int Size, int Control) ThrowInvalidFrame() => throw new InvalidDataException("Invalid binary cache frame.");
}
