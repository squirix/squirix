using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>The value of one server entry. Scalars live inline and variable-length payloads own their bytes.</summary>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct StoredValue
{
    /// <summary>Identifies an inline integer whose full 64 bits are available for data.</summary>
    private static readonly byte[] IntegerMarker = new byte[1];
    private static readonly byte[] DoubleMarker = new byte[1];

    private readonly byte[]? _payload;

    private StoredValue(byte[]? payload, long bitsOrKind)
    {
        _payload = payload;
        Integer = bitsOrKind;
    }

    internal bool IsPageRecord => IsSlice && (Integer & (1L << 24)) != 0;

    internal StoredValueKind Kind => _payload switch
    {
        null => Integer == 0 ? StoredValueKind.Null : StoredValueKind.Boolean,
        var marker when ReferenceEquals(marker, IntegerMarker) => StoredValueKind.Integer,
        var marker when ReferenceEquals(marker, DoubleMarker) => StoredValueKind.Double,
        _ => Unsafe.BitCast<long, StoredValueKind>(IsSlice ? Integer & 7 : Integer),
    };

    internal ReadOnlyMemory<byte> Memory => IsSlice
        ? _payload!.AsMemory(int.CreateTruncating(Integer >> 32) & int.MaxValue, int.CreateTruncating(Integer >>> 3) & 65535)
        : OwnedBytes;

    internal byte[] Bytes => IsSlice ? Memory.ToArray() : OwnedBytes;

    internal bool Boolean => Integer == 2;

    internal long Integer { get; }

    internal double Double => BitConverter.Int64BitsToDouble(Integer);

    internal long EstimatedPayloadBytes => Kind switch
    {
        StoredValueKind.Null => 0,
        StoredValueKind.Boolean => sizeof(bool),
        StoredValueKind.Integer or StoredValueKind.Double => sizeof(long),
        StoredValueKind.Json => Memory.Length + JsonUtf8Value.OverheadBytes,
        _ => Memory.Length,
    };

    internal bool IsSlice => Integer < 0 && _payload != null && !ReferenceEquals(_payload, IntegerMarker) && !ReferenceEquals(_payload, DoubleMarker);

    private byte[] OwnedBytes => Kind is StoredValueKind.Text or StoredValueKind.Binary or StoredValueKind.Json ? _payload! : [];

    internal static StoredValue FromPage(StoredValueKind kind, byte[] page, int offset, int length, bool record = false) =>
        new(page, long.MinValue | (record ? 1L << 24 : 0) | (long.CreateChecked(offset) << 32) | (long.CreateChecked(length) << 3) | Unsafe.BitCast<StoredValueKind, long>(kind));

    internal static StoredValue FromBoolean(bool value) => new(null, value ? 2 : 1);

    internal static StoredValue FromInteger(long value) => new(IntegerMarker, value);

    internal static StoredValue FromDouble(double value) => new(DoubleMarker, BitConverter.DoubleToInt64Bits(value));

    internal static StoredValue FromText(string text) => FromText(Encoding.UTF8.GetBytes(text));

    internal static StoredValue FromText(byte[] owned) => new(owned, Unsafe.BitCast<StoredValueKind, long>(StoredValueKind.Text));

    internal static StoredValue FromBinary(byte[] owned) => new(owned, Unsafe.BitCast<StoredValueKind, long>(StoredValueKind.Binary));

    internal static StoredValue FromJson(ReadOnlyMemory<byte> json)
    {
        var bytes = MemoryMarshal.TryGetArray(json, out var slice) && slice.Array is { } array && slice.Count == array.Length ? array : json.ToArray();
        return new StoredValue(bytes, Unsafe.BitCast<StoredValueKind, long>(StoredValueKind.Json));
    }

    internal StoredValue ReadSlice(StoredValueKind kind, int relativeOffset, int length) =>
        FromPage(kind, _payload!, (int.CreateTruncating(Integer >> 32) & int.MaxValue) + relativeOffset, length);
}
