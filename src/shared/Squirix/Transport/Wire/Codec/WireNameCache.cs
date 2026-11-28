using System;
using System.Text;
using System.Threading;

namespace Squirix.Transport.Wire.Codec;

/// <summary>Remembers the last short cache name decoded on one connection.</summary>
internal sealed class WireNameCache
{
    private string? _last;

    internal string? Find(ReadOnlySpan<byte> bytes)
    {
        var last = Volatile.Read(ref _last);
        return last != null && bytes.Length == last.Length && Ascii.Equals(bytes, last) ? last : null;
    }

    internal void Remember(string name)
    {
        if (name.Length <= 128)
            Volatile.Write(ref _last, name);
    }
}
