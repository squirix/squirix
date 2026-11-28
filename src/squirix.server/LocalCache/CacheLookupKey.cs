using System;

namespace Squirix.Server.LocalCache;

/// <summary>A temporary lookup key. It holds a text key or the original UTF-8 bytes.</summary>
internal readonly ref struct CacheLookupKey
{
    internal CacheLookupKey(string cacheName, ReadOnlySpan<char> chars, ReadOnlySpan<byte> utf8 = default)
    {
        CacheName = cacheName;
        Chars = chars;
        Utf8 = utf8;
    }

    internal string CacheName { get; }

    internal ReadOnlySpan<char> Chars { get; }

    internal ReadOnlySpan<byte> Utf8 { get; }
}
