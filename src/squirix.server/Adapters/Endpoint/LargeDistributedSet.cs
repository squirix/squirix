using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Locates the borrowed value of a canonical, large distributed write.</summary>
internal static class LargeDistributedSet
{
    private const int MaxMetadata = 4096;
    private const int Threshold = 64 * 1024;

    internal static bool TryParse(ReadOnlyMemory<byte> body, [NotNullWhen(true)] out SetDistributedAsyncRequest? metadata, out ReadOnlyMemory<byte> value)
    {
        metadata = null;
        value = default;
        if (body.Length < Threshold)
            return false;

        var rest = body.Span;
        if (!TryDelimited(ref rest, 10, out _) || !TryDelimited(ref rest, 18, out _))
            return false;

        var prefixLength = body.Length - rest.Length;
        if (!TryDelimited(ref rest, 26, out var valueLength) || valueLength < Threshold)
            return false;

        var suffixOffset = body.Length - rest.Length;
        var metadataLength = prefixLength + rest.Length;
        if (metadataLength > MaxMetadata || !HasCanonicalSuffix(rest))
            return false;

        Span<byte> wire = stackalloc byte[metadataLength];
        body.Span[..prefixLength].CopyTo(wire);
        rest.CopyTo(wire[prefixLength..]);
        metadata = SetDistributedAsyncRequest.Parser.ParseFrom(wire);
        value = body.Slice(suffixOffset - valueLength, valueLength);
        return true;
    }

    private static bool HasCanonicalSuffix(ReadOnlySpan<byte> rest)
    {
        var previous = 26;
        while (!rest.IsEmpty)
        {
            var tag = rest[0];
            if (tag <= previous)
                return false;
            previous = tag;
            if (!TrySkipSuffixField(ref rest, tag))
                return false;
        }

        return true;
    }

    private static bool TryDelimited(ref ReadOnlySpan<byte> rest, byte tag, out int length)
    {
        length = 0;
        if (rest.IsEmpty || rest[0] != tag)
            return false;

        rest = rest[1..];
        if (!TryVarint(ref rest, out var count) || count > ulong.CreateChecked(rest.Length))
            return false;

        length = int.CreateChecked(count);
        rest = rest[length..];
        return true;
    }

    private static bool TrySkipSuffixField(ref ReadOnlySpan<byte> rest, byte tag)
    {
        if (tag == 50)
            return TryDelimited(ref rest, tag, out _);

        if (tag is not (32 or 40 or 56))
            return false;

        rest = rest[1..];
        return TryVarint(ref rest, out _);
    }

    private static bool TryVarint(ref ReadOnlySpan<byte> rest, out ulong value)
    {
        value = 0;
        var count = Math.Min(rest.Length, 10);
        for (var i = 0; i < count; i++)
        {
            var next = rest[i];
            if (i == 9 && next > 1)
                return false;
            value |= ulong.CreateChecked(next & 127) << (i * 7);
            if (next >= 128)
                continue;
            rest = rest[(i + 1)..];
            return true;
        }

        return false;
    }
}
