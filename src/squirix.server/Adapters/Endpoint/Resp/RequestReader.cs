using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Adapters.Endpoint.Resp;

internal sealed class RequestReader
{
    internal const int MaxBulkBytes = EntryLimits.MaxRespBulkBytes;
    internal const int MaxFrameBytes = MaxBulkBytes + (16 * 1024);
    internal const int MaxArguments = 8;

    private const byte Cr = 13;
    private const byte Lf = 10;
    private const int InitialBufferBytes = 4096;
    private readonly Range[] _arguments = new Range[MaxArguments];
    private byte[] _buffer = new byte[InitialBufferBytes];
    private int _start;
    private int _end;
    private int _frameBytes;

    internal int Count { get; private set; }

    /// <summary>Gets an argument slice valid until the next request is read.</summary>
    /// <param name="index">The zero-based argument position.</param>
    internal ReadOnlyMemory<byte> this[int index] => _buffer.AsMemory(_start, _frameBytes)[_arguments[index]];

    internal bool IsCommand(int index, ReadOnlySpan<byte> command) => Ascii.EqualsIgnoreCase(this[index].Span, command);

    internal async ValueTask<bool> ReadAsync(Stream stream, CancellationToken cancellationToken)
    {
        _start += _frameBytes;
        _frameBytes = 0;
        Count = 0;

        if (_start == _end)
        {
            _start = 0;
            _end = 0;
            if (_buffer.Length > InitialBufferBytes)
                _buffer = new byte[InitialBufferBytes];
        }

        while (true)
        {
            if (TryParseFrame())
                return true;

            MakeReadSpace();
            var read = await stream.ReadAsync(_buffer.AsMemory(_end), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                return _end == _start ? false : throw new InvalidDataException("Incomplete RESP request.");

            _end += read;
        }
    }

    internal bool TryReadBuffered()
    {
        _start += _frameBytes;
        _frameBytes = 0;
        Count = 0;
        return _start != _end && TryParseFrame();
    }

    private static bool TryReadHeader(ReadOnlySpan<byte> bytes, ref int position, byte prefix, int maximum, out int length)
    {
        length = 0;
        if (position == bytes.Length)
            return false;
        if (bytes[position++] != prefix)
            throw new InvalidDataException("Unexpected RESP request type.");

        var start = position;
        while (position < bytes.Length)
        {
            var next = bytes[position++];
            if (next == Cr)
                return ReadHeaderTerminator(bytes, ref position, start);

            length = ReadLengthDigit(length, next, position - start, maximum);
        }

        return false;
    }

    private static int ReadLengthDigit(int length, byte next, int digits, int maximum)
    {
        var digit = next - '0';
        if (digit is < 0 or > 9 || digits > 7)
            throw new InvalidDataException("Invalid RESP length header.");
        length = (length * 10) + digit;
        return length <= maximum ? length : throw new InvalidDataException("RESP request exceeds its length limit.");
    }

    private static bool ReadHeaderTerminator(ReadOnlySpan<byte> bytes, ref int position, int start) =>
        position < bytes.Length && (position - start > 1 && bytes[position++] == Lf ? true : throw new InvalidDataException("Invalid RESP length header."));

    private static bool TryReadBulk(ReadOnlySpan<byte> bytes, ref int position, out Range argument)
    {
        argument = default;
        if (!TryReadHeader(bytes, ref position, "$"u8[0], MaxBulkBytes, out var length))
            return false;

        var payloadEnd = position + length;
        if (payloadEnd + 2 > MaxFrameBytes)
            throw new InvalidDataException("RESP request exceeds the frame limit.");
        if (payloadEnd + 2 > bytes.Length)
            return false;
        if (bytes[payloadEnd] != Cr || bytes[payloadEnd + 1] != Lf)
            throw new InvalidDataException("Invalid RESP bulk terminator.");

        argument = position..payloadEnd;
        position = payloadEnd + 2;
        return true;
    }

    private bool TryParseFrame()
    {
        var bytes = _buffer.AsSpan(_start, _end - _start);
        var position = 0;
        if (!TryReadHeader(bytes, ref position, "*"u8[0], MaxArguments, out var count))
            return false;
        if (count == 0)
            throw new InvalidDataException("Empty RESP request array.");

        for (var index = 0; index < count; index++)
        {
            if (!TryReadBulk(bytes, ref position, out _arguments[index]))
                return false;
        }

        Count = count;
        _frameBytes = position;
        return true;
    }

    private void MakeReadSpace()
    {
        if (_end < _buffer.Length)
            return;

        var pending = _end - _start;
        if (pending == MaxFrameBytes)
            throw new InvalidDataException("RESP request exceeds the frame limit.");
        if (_start != 0)
        {
            _buffer.AsSpan(_start, pending).CopyTo(_buffer);
            _start = 0;
            _end = pending;
            return;
        }

        Array.Resize(ref _buffer, Math.Min(_buffer.Length * 2, MaxFrameBytes));
    }
}
