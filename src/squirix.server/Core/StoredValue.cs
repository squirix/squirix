using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Squirix.Server.Core;

/// <summary>The value of one server entry. Scalars live inline and variable-length payloads own their bytes.</summary>
[StructLayout(LayoutKind.Auto)]
internal readonly struct StoredValue
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

    internal enum ValueKind : long
    {
        /// <summary>Null value.</summary>
        Null = 0,

        /// <summary>Boolean value.</summary>
        Boolean = 1,

        /// <summary>Integer value.</summary>
        Integer = 2,

        /// <summary>Double value.</summary>
        Double = 3,

        /// <summary>Text value.</summary>
        Text = 4,

        /// <summary>Binary value.</summary>
        Binary = 5,

        /// <summary>Json value.</summary>
        Json = 6,
    }

    internal ValueKind Kind => _payload switch
    {
        null => Integer == 0 ? ValueKind.Null : ValueKind.Boolean,
        var marker when ReferenceEquals(marker, IntegerMarker) => ValueKind.Integer,
        var marker when ReferenceEquals(marker, DoubleMarker) => ValueKind.Double,
        _ => Unsafe.BitCast<long, ValueKind>(Integer),
    };

    internal byte[] Bytes => Kind is ValueKind.Text or ValueKind.Binary or ValueKind.Json ? _payload! : [];

    internal bool Boolean => Integer == 2;

    internal long Integer { get; }

    internal double Double => BitConverter.Int64BitsToDouble(Integer);

    internal long EstimatedPayloadBytes => Kind switch
    {
        ValueKind.Null => 0,
        ValueKind.Boolean => sizeof(bool),
        ValueKind.Integer or ValueKind.Double => sizeof(long),
        ValueKind.Json => Bytes.LongLength + JsonUtf8Value.OverheadBytes,
        _ => Bytes.LongLength,
    };

    internal static StoredValue FromBoolean(bool value) => new(null, value ? 2 : 1);

    internal static StoredValue FromInteger(long value) => new(IntegerMarker, value);

    internal static StoredValue FromDouble(double value) => new(DoubleMarker, BitConverter.DoubleToInt64Bits(value));

    internal static StoredValue FromText(string text) => FromText(Encoding.UTF8.GetBytes(text));

    internal static StoredValue FromText(byte[] owned) => new(owned, Unsafe.BitCast<ValueKind, long>(ValueKind.Text));

    internal static StoredValue FromBinary(byte[] owned) => new(owned, Unsafe.BitCast<ValueKind, long>(ValueKind.Binary));

    internal static StoredValue FromJson(ReadOnlyMemory<byte> json)
    {
        var bytes = MemoryMarshal.TryGetArray(json, out var slice) && slice.Array is { } array && slice.Count == array.Length ? array : json.ToArray();
        return new StoredValue(bytes, Unsafe.BitCast<ValueKind, long>(ValueKind.Json));
    }
}
