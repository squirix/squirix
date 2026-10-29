using System;
using System.IO;
using Google.Protobuf;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Uses protobuf primitives while retaining entry and snapshot byte slices in the owned transport buffer.</summary>
internal static class ReplicaTransferParser
{
    internal static TransferReplicaStateRequest Parse(byte[] buffer, int length)
    {
        var request = new TransferReplicaStateRequest();
        using var input = new CodedInputStream(buffer, 0, length);
        while (!input.IsAtEnd)
        {
            var start = int.CreateChecked(input.Position);
            switch (input.ReadTag())
            {
                case 10:
                    input.ReadMessage(request.Header ??= new ReplicationEnvelopeHeader());
                    break;
                case 18:
                    request.TransferId = input.ReadString();
                    break;
                case 24:
                    request.Sequence = input.ReadUInt32();
                    break;
                case 34:
                    input.ReadMessage(request.Begin ??= new ReplicaTransferBegin());
                    break;
                case 42:
                    ReadSnapshot(ReadSegment(input, buffer), request.Snapshot ??= new ReplicaTransferSnapshot());
                    break;
                case 50:
                    input.ReadMessage(request.Wipe ??= new ReplicaTransferWipe());
                    break;
                case 58:
                    ReadEntries(ReadSegment(input, buffer), request.Entries ??= new ReplicaTransferEntries());
                    break;
                case 66:
                    input.ReadMessage(request.End ??= new ReplicaTransferEnd());
                    break;
                case 74:
                    input.ReadMessage(request.Abort ??= new ReplicaTransferAbort());
                    break;
                default:
                    PreserveUnknown(input, buffer, start, request);
                    break;
            }
        }

        return request;
    }

    private static ReplicaTransferEntry ReadEntry(ArraySegment<byte> bytes)
    {
        var entry = new ReplicaTransferEntry();
        using var input = new CodedInputStream(bytes.Array!, bytes.Offset, bytes.Count);
        while (!input.IsAtEnd)
        {
            var start = int.CreateChecked(input.Position);
            switch (input.ReadTag())
            {
                case 10:
                    entry.CacheName = input.ReadString();
                    break;
                case 18:
                    entry.Key = UnsafeByteOperations.UnsafeWrap(ReadSegment(input, bytes.Array!).AsMemory());
                    break;
                case 26:
                    entry.Payload = UnsafeByteOperations.UnsafeWrap(ReadSegment(input, bytes.Array!).AsMemory());
                    break;
                case 32:
                    entry.ExpiresUtcTicks = input.ReadInt64();
                    break;
                default:
                    PreserveUnknown(input, bytes.Array!, start, entry, 2);
                    break;
            }
        }

        return entry;
    }

    private static void ReadEntries(ArraySegment<byte> bytes, ReplicaTransferEntries entries)
    {
        using var input = new CodedInputStream(bytes.Array!, bytes.Offset, bytes.Count);
        while (!input.IsAtEnd)
        {
            var start = int.CreateChecked(input.Position);
            if (input.ReadTag() == 10)
                entries.Entries.Add(ReadEntry(ReadSegment(input, bytes.Array!)));
            else
                PreserveUnknown(input, bytes.Array!, start, entries, 1);
        }
    }

    private static void ReadSnapshot(ArraySegment<byte> bytes, ReplicaTransferSnapshot snapshot)
    {
        using var input = new CodedInputStream(bytes.Array!, bytes.Offset, bytes.Count);
        while (!input.IsAtEnd)
        {
            var start = int.CreateChecked(input.Position);
            switch (input.ReadTag())
            {
                case 8:
                    snapshot.Offset = input.ReadUInt64();
                    break;
                case 18:
                    snapshot.Chunk = UnsafeByteOperations.UnsafeWrap(ReadSegment(input, bytes.Array!).AsMemory());
                    break;
                default:
                    PreserveUnknown(input, bytes.Array!, start, snapshot, 1);
                    break;
            }
        }
    }

    private static ArraySegment<byte> ReadSegment(CodedInputStream input, byte[] buffer)
    {
        var start = int.CreateChecked(input.Position);
        input.SkipLastField();
        var end = int.CreateChecked(input.Position);
        while ((buffer[start] & 0x80) != 0)
            start++;
        start++;

        return new ArraySegment<byte>(buffer, start, end - start);
    }

    private static void PreserveUnknown(CodedInputStream input, byte[] buffer, int start, IMessage message, int depth = 0)
    {
        // Generated unknown-field parsing accepts the full recursion budget; SkipLastField has a different boundary.
        if ((buffer[start] & 7) == 3)
            _ = UnknownFieldSet.MergeFieldFrom(null, input);
        else
            input.SkipLastField();
        var length = int.CreateChecked(input.Position) - start;

        // Borrowed submessages use separate readers, so include their enclosing message depth in the group budget.
        ValidateGroupDepth(buffer, start, length, depth, input.RecursionLimit);
        message.MergeFrom(buffer.AsSpan(start, length));
    }

    private static void ValidateGroupDepth(byte[] buffer, int start, int length, int depth, int limit)
    {
        if ((buffer[start] & 7) != 3)
            return;

        using var input = new CodedInputStream(buffer, start, length);
        while (!input.IsAtEnd)
        {
            var wireType = WireFormat.GetTagWireType(input.ReadTag());
            if (wireType == WireFormat.WireType.StartGroup)
            {
                if (++depth > limit)
                    throw new InvalidDataException("The state transfer protobuf message exceeds the recursion limit.");
            }
            else if (wireType == WireFormat.WireType.EndGroup)
            {
                depth--;
            }
            else
            {
                input.SkipLastField();
            }
        }
    }
}
