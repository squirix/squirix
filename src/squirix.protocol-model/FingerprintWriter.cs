using System;
using System.Globalization;

namespace Squirix.ProtocolModel;

/// <summary>Builds a state fingerprint in a reusable character buffer, so a state already seen can be looked up without allocating its string.</summary>
/// <remarks>Not thread-safe. The span returned by <see cref="AsSpan"/> is valid until the next write or <see cref="Clear"/>.</remarks>
internal sealed class FingerprintWriter
{
    private const int MaxIntChars = 11;

    private char[] _buffer;
    private int _length;

    internal FingerprintWriter(int initialCapacity)
    {
        _buffer = new char[initialCapacity];
    }

    public override string ToString() => new(_buffer, 0, _length);

    internal void Append(char value)
    {
        EnsureCapacity(1);
        _buffer[_length++] = value;
    }

    internal void Append(int value)
    {
        EnsureCapacity(MaxIntChars);
        _ = value.TryFormat(_buffer.AsSpan(_length), out var written, default, CultureInfo.InvariantCulture);
        _length += written;
    }

    internal void AppendFlag(bool value) => Append(value ? '1' : '0');

    internal ReadOnlySpan<char> AsSpan() => new(_buffer, 0, _length);

    internal void Clear() => _length = 0;

    private void EnsureCapacity(int additional)
    {
        if (_buffer.Length - _length >= additional)
            return;

        Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, _length + additional));
    }
}
