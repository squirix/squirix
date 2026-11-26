using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Core;

/// <summary>Encodes a value as kind → fixed-width scalar or length-prefixed bytes.</summary>
internal static class CompactPageValue
{
    /// <summary>Escape byte followed by a full 32-bit payload length.</summary>
    private const byte LongLength = byte.MaxValue;

    internal static Encoding KeyEncoding { get; } = new UTF8Encoding(false, true);

    internal static int EncodedLength(in StoredValue value) => value.Kind switch
    {
        StoredValueKind.Null => 1,
        StoredValueKind.Boolean => 2,
        StoredValueKind.Integer or StoredValueKind.Double => 1 + sizeof(long),
        StoredValueKind.Text or StoredValueKind.Binary or StoredValueKind.Json => BlobSize(value.Memory.Length),
        _ => throw new InvalidOperationException("Unknown stored value kind."),
    };

    internal static void Write(Span<byte> destination, in StoredValue value)
    {
        var kind = value.Kind;
        destination[0] = byte.CreateChecked(Unsafe.BitCast<StoredValueKind, long>(kind));
        var body = destination[1..];
        switch (kind)
        {
            case StoredValueKind.Null:
                return;
            case StoredValueKind.Boolean:
                body[0] = Convert.ToByte(value.Boolean);
                return;
            case StoredValueKind.Integer:
            case StoredValueKind.Double:
                BinaryPrimitives.WriteInt64LittleEndian(body, value.Integer);
                return;
            case StoredValueKind.Text:
            case StoredValueKind.Binary:
            case StoredValueKind.Json:
                WriteBlob(body, value.Memory.Span);
                return;
            default:
                throw new InvalidOperationException("Unknown stored value kind.");
        }
    }

    internal static StoredValue Read(ReadOnlySpan<byte> source)
    {
        var kind = Unsafe.BitCast<long, StoredValueKind>(source[0]);
        var body = source[1..];
        return kind switch
        {
            StoredValueKind.Null => default,
            StoredValueKind.Boolean => StoredValue.FromBoolean(body[0] != 0),
            StoredValueKind.Integer => StoredValue.FromInteger(BinaryPrimitives.ReadInt64LittleEndian(body)),
            StoredValueKind.Double => StoredValue.FromDouble(BinaryPrimitives.ReadDoubleLittleEndian(body)),
            StoredValueKind.Text => StoredValue.FromText(ReadOwnedBlob(body)),
            StoredValueKind.Binary => StoredValue.FromBinary(ReadOwnedBlob(body)),
            StoredValueKind.Json => StoredValue.FromJson(ReadOwnedBlob(body)),
            _ => throw new InvalidOperationException("Unknown compact page value tag."),
        };
    }

    internal static StoredValue ReadRecord(in StoredValue record, int offset)
    {
        var source = record.Memory.Span[offset..];
        var kind = Unsafe.BitCast<long, StoredValueKind>(source[0]);
        if (kind is not (StoredValueKind.Text or StoredValueKind.Binary or StoredValueKind.Json))
            return Read(source);

        var length = source[1];
        return length == LongLength
            ? record.ReadSlice(kind, offset + 6, BinaryPrimitives.ReadInt32LittleEndian(source[2..]))
            : record.ReadSlice(kind, offset + 2, length);
    }

    private static int BlobSize(int length) => checked(2 + length + (length < LongLength ? 0 : sizeof(int)));

    private static void WriteBlob(Span<byte> destination, ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < LongLength)
        {
            destination[0] = byte.CreateChecked(bytes.Length);
            bytes.CopyTo(destination[1..]);
            return;
        }

        destination[0] = LongLength;
        BinaryPrimitives.WriteInt32LittleEndian(destination[1..], bytes.Length);
        bytes.CopyTo(destination[5..]);
    }

    private static byte[] ReadOwnedBlob(ReadOnlySpan<byte> body) => body[0] == LongLength
        ? BufferEx.CopyToOwned(body.Slice(5, BinaryPrimitives.ReadInt32LittleEndian(body[1..])))
        : BufferEx.CopyToOwned(body.Slice(1, body[0]));
}
