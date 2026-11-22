using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Text.Unicode;

namespace Squirix.Transport.Wire.Codec;

internal ref struct WireWriter
{
    private readonly Span<byte> _destination;
    private readonly IBufferWriter<byte>? _output;
    private readonly bool _hasDestination;

    internal WireWriter(string? mutationOperationId)
    {
        MutationOperationId = mutationOperationId;
    }

    internal WireWriter(Span<byte> destination, string? mutationOperationId = null)
    {
        _destination = destination;
        _hasDestination = true;
        MutationOperationId = mutationOperationId;
    }

    internal WireWriter(IBufferWriter<byte> output)
    {
        _output = output;
    }

    internal int Written { get; private set; }

    internal string? MutationOperationId { get; }

    internal void Byte(byte value)
    {
        if (_hasDestination)
        {
            _destination[Written] = value;
        }
        else if (_output != null)
        {
            _output.GetSpan(1)[0] = value;
            _output.Advance(1);
        }

        Written = checked(Written + 1);
    }

    internal void Boolean(bool value) => Byte(value ? byte.CreateTruncating(1) : byte.MinValue);

    internal void Unsigned(ulong value)
    {
        if (value < 128)
        {
            Byte(byte.CreateTruncating(value));
            return;
        }

        var length = (BitOperations.Log2(value) / 7) + 1;
        if (_hasDestination || _output != null)
        {
            var bytes = _output == null ? _destination.Slice(Written, length) : _output.GetSpan(length);
            var offset = 0;
            while (value >= 128)
            {
                bytes[offset++] = byte.CreateTruncating(value | 128);
                value >>= 7;
            }

            bytes[offset] = byte.CreateTruncating(value);
            _output?.Advance(length);
        }

        Written = checked(Written + length);
    }

    internal void ItemCount(int count, int maximum = WireReader.MaxItems)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, maximum);
        Unsigned(ulong.CreateChecked(count));
    }

    internal void UInt32(uint value) => Unsigned(value);

    internal void Signed(long value) => Unsigned(ulong.CreateTruncating((value << 1) ^ (value >> 63)));

    internal void Double(double value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleLittleEndian(buffer, value);
        Raw(buffer);
    }

    internal void Duration(TimeSpan value) => Signed(value.Ticks);

    internal void Timestamp(DateTime value) => Signed(value.Ticks);

    internal void Bytes(ReadOnlyMemory<byte> value)
    {
        Unsigned(ulong.CreateChecked(value.Length));
        Raw(value.Span);
    }

    internal void TailBytes(ReadOnlyMemory<byte> value) => Raw(value.Span);

    internal void OperationId(string value)
    {
        if (!WireReader.IsCompactId(value))
        {
            Text(value, 1);
            return;
        }

        Byte(0);
        if (_hasDestination || _output != null)
        {
            Span<byte> bytes = stackalloc byte[16];
            _ = Convert.FromHexString(value, bytes, out _, out _);
            Raw(bytes);
        }
        else
        {
            Written = checked(Written + 16);
        }
    }

    internal void TraceParent(in ActivityContext parent)
    {
        if (_hasDestination || _output != null)
        {
            var bytes = _output == null ? _destination.Slice(Written, WireTraceParent.ByteCount) : _output.GetSpan(WireTraceParent.ByteCount);
            WireTraceParent.Write(bytes, in parent);
            _output?.Advance(WireTraceParent.ByteCount);
        }

        Written = checked(Written + WireTraceParent.ByteCount);
    }

    internal void Text(string value, bool tail = false) => Text(value, 0, tail);

    internal void Raw(scoped ReadOnlySpan<byte> value)
    {
        if (_hasDestination)
        {
            value.CopyTo(_destination[Written..]);
        }
        else if (_output != null)
        {
            var rest = value;
            while (!rest.IsEmpty)
            {
                var destination = _output.GetSpan();
                var length = Math.Min(destination.Length, rest.Length);
                rest[..length].CopyTo(destination);
                _output.Advance(length);
                rest = rest[length..];
            }
        }

        Written = checked(Written + value.Length);
    }

    private void Text(string value, int lengthBias, bool tail = false)
    {
        var size = WireReader.Utf8.GetByteCount(value);
        if (!tail)
            Unsigned(ulong.CreateChecked(size) + uint.CreateChecked(lengthBias));
        if (_hasDestination)
        {
            _ = WireReader.Utf8.GetBytes(value, _destination[Written..]);
        }
        else if (_output != null && size > 0)
        {
            var chunk = _output.GetSpan();
            if (chunk.Length >= size)
            {
                _ = WireReader.Utf8.GetBytes(value, chunk);
                _output.Advance(size);
            }
            else
            {
                var rest = value.AsSpan();
                while (!rest.IsEmpty)
                {
                    // GetByteCount validated the string. Four bytes always fit its next Unicode scalar.
                    _ = Utf8.FromUtf16(rest, _output.GetSpan(4), out var used, out var bytes, false);
                    _output.Advance(bytes);
                    rest = rest[used..];
                }
            }
        }

        Written = checked(Written + size);
    }
}
