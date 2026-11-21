using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Squirix.Transport.Wire.Codec;

internal ref struct WireReader
{
    internal const int MaxItems = 65536;

    internal static readonly UTF8Encoding Utf8 = new(false, true);
    private static readonly SearchValues<char> Hex = SearchValues.Create("0123456789abcdef");
    private readonly ReadOnlySpan<byte> _frame;
    private readonly ReadOnlyMemory<byte> _borrowed;
    private int _offset;

    internal WireReader(ReadOnlySpan<byte> frame, ReadOnlyMemory<byte> borrowed = default)
    {
        _frame = frame;
        _borrowed = borrowed;
    }

    internal readonly int Remaining => _frame.Length - _offset;

    internal static byte[] CopyBytes(ReadOnlySpan<byte> source)
    {
        if (source.IsEmpty)
            return [];

        var copy = GC.AllocateUninitializedArray<byte>(source.Length);
        source.CopyTo(copy);
        return copy;
    }

    internal static bool IsCompactId(string value) => value.Length == 32 && !value.AsSpan().ContainsAnyExcept(Hex);

    internal byte Byte() => _offset < _frame.Length ? _frame[_offset++] : throw Invalid();

    internal int Flags(int mask)
    {
        var value = Byte();
        return (value & ~mask) == 0 ? value : throw Invalid();
    }

    internal bool Boolean() => Flags(1) != 0;

    internal ulong Unsigned()
    {
        ulong value = 0;
        for (var shift = 0; shift <= 63; shift += 7)
        {
            var b = Byte();
            if (shift == 63 && b > 1)
                throw Invalid();

            value |= ulong.CreateTruncating(b & 127) << shift;
            if (b < 128)
                return b != 0 || shift == 0 ? value : throw Invalid();
        }

        throw Invalid();
    }

    internal uint UInt32()
    {
        var value = Unsigned();
        return value <= uint.MaxValue ? uint.CreateTruncating(value) : throw Invalid();
    }

    internal long Signed()
    {
        var value = Unsigned();
        return long.CreateTruncating(value >> 1) ^ -long.CreateTruncating(value & 1);
    }

    internal int Count()
    {
        var length = Unsigned();
        return length <= uint.CreateTruncating(Remaining) ? int.CreateTruncating(length) : throw Invalid();
    }

    internal int ItemCount(int maximum = MaxItems)
    {
        var count = Count();
        return count <= maximum ? count : throw Invalid();
    }

    internal ReadOnlyMemory<byte> Bytes(out byte[]? owned) => ReadBytes(Count(), out owned);

    internal ReadOnlyMemory<byte> TailBytes(out byte[]? owned) => ReadBytes(Remaining, out owned);

    internal string OperationId()
    {
        var length = Unsigned();
        if (length != 0)
        {
            if (length - 1 > uint.CreateChecked(Remaining))
                throw Invalid();

            var value = Text(int.CreateChecked(length - 1));
            return IsCompactId(value) ? throw Invalid() : value;
        }

        if (Remaining < 16)
            throw Invalid();

        var id = Convert.ToHexStringLower(_frame.Slice(_offset, 16));
        _offset += 16;
        return id;
    }

    internal string Text(bool tail = false) => Text(tail ? Remaining : Count());

    internal double Double()
    {
        if (Remaining < sizeof(double))
            throw Invalid();

        var number = BinaryPrimitives.ReadDoubleLittleEndian(_frame[_offset..]);
        _offset += sizeof(double);
        return number;
    }

    internal TimeSpan Duration() => TimeSpan.FromTicks(Signed());

    internal DateTime Timestamp()
    {
        var ticks = Signed();
        return ticks >= 0 && ticks <= DateTime.MaxValue.Ticks ? new DateTime(ticks, DateTimeKind.Utc) : throw Invalid();
    }

    internal ReadOnlyMemory<byte> ReadBytes(int length, out byte[]? owned)
    {
        if (length < 0 || length > Remaining)
            throw Invalid();

        var offset = _offset;
        _offset += length;
        if (!_borrowed.IsEmpty)
        {
            owned = null;
            return _borrowed.Slice(offset, length);
        }

        owned = CopyBytes(_frame.Slice(offset, length));
        return owned;
    }

    private static InvalidDataException Invalid() => new("Malformed binary message.");

    private string Text(int length)
    {
        var text = _frame.Slice(_offset, length);
        _offset += length;
        try
        {
            return Utf8.GetString(text);
        }
        catch (DecoderFallbackException error)
        {
            throw new InvalidDataException("Invalid UTF-8 in a wire string.", error);
        }
    }
}
