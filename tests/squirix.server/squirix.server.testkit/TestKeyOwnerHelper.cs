using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Squirix.Attributes;

namespace Squirix.Server.TestKit;

/// <summary>Key ownership helper mirroring server consistent-hash route behavior for multi-node tests.</summary>
[Immutable]
public sealed class TestKeyOwnerHelper
{
    /// <summary>Shared ring for smoke tests that label peers <c>A</c> and <c>B</c>.</summary>
    public static readonly TestKeyOwnerHelper SmokeTwoNode = new(["A", "B"]);

    /// <summary>Shared ring for the default two-node topology (<c>node-a</c>, <c>node-b</c>).</summary>
    public static readonly TestKeyOwnerHelper TwoNode = new(["node-a", "node-b"]);

    private readonly (ulong Hash, string Node)[] _ring;

    /// <summary>Initializes a new instance of the <see cref="TestKeyOwnerHelper" /> class.</summary>
    /// <param name="nodeIds">Node identifiers participating in the ring.</param>
    /// <param name="virtualNodes">Number of virtual nodes per physical node.</param>
    private TestKeyOwnerHelper(ReadOnlySpan<string> nodeIds, int virtualNodes = 128)
    {
        var uniqueNodes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var nodeId in nodeIds)
        {
            if (!string.IsNullOrWhiteSpace(nodeId))
                _ = uniqueNodes.Add(nodeId);
        }

        if (uniqueNodes.Count == 0)
            throw new ArgumentException("At least one node is required.", nameof(nodeIds));

        var nodes = new string[uniqueNodes.Count];
        uniqueNodes.CopyTo(nodes);

        var ring = new (ulong Hash, string Node)[nodes.Length * virtualNodes];
        var writeIndex = 0;
        for (var i = 0; i < nodes.Length; i++)
        {
            var node = nodes[i];
            for (var vnode = 0; vnode < virtualNodes; vnode++)
                ring[writeIndex++] = (HashVNode(node, vnode), node);
        }

        Array.Sort(ring, static (a, b) => a.Hash.CompareTo(b.Hash));
        _ring = ring;
    }

    /// <summary>Returns a cache key owned by <paramref name="ownerId" />.</summary>
    /// <param name="cacheName">Cache name used for routing.</param>
    /// <param name="ownerId">Expected owning node identifier.</param>
    /// <param name="prefix">Key prefix used while searching.</param>
    /// <returns>A key routed to <paramref name="ownerId" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no matching key is found within the search budget.</exception>
    public string FindKeyOwnedBy(string cacheName, string ownerId, string prefix)
    {
        for (var i = 0; i < 200_000; i++)
        {
            var candidate = NodeInvariantIndexStrings.FormatPrefixed(prefix, i);
            if (string.Equals(GetOwner(cacheName, candidate), ownerId, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException("Unable to find a key owned by the requested node.");
    }

    private static int CountDigits(int value)
    {
        var digits = 1;
        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }

    private static ulong HashBytes(ReadOnlySpan<byte> bytes)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(bytes, digest);
        return BitConverter.ToUInt64(digest);
    }

    private static ulong HashCacheRouteKey(string cacheName, string key)
    {
        var canonical = string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
        var byteCount = checked(CountDigits(canonical.Length) + 1 + Encoding.UTF8.GetByteCount(canonical) + 1 + Encoding.UTF8.GetByteCount(key));
        if (byteCount <= 512)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            WriteRouteKey(canonical, key, buffer);
            return HashBytes(buffer);
        }

        var owned = new byte[byteCount];
        WriteRouteKey(canonical, key, owned);
        return HashBytes(owned);
    }

    private static ulong HashVNode(string node, int index)
    {
        var byteCount = checked(Encoding.UTF8.GetByteCount(node) + 1 + CountDigits(index));
        if (byteCount <= 512)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            return HashBytes(WriteVNodeKey(node, index, buffer));
        }

        var owned = new byte[byteCount];
        return HashBytes(WriteVNodeKey(node, index, owned));
    }

    private static int WriteNonNegativeIntUtf8(int value, Span<byte> destination)
    {
        var digitsUtf8 = "0123456789"u8;
        var digits = CountDigits(value);
        for (var i = digits - 1; i >= 0; i--)
        {
            destination[i] = digitsUtf8[value % 10];
            value /= 10;
        }

        return digits;
    }

    private static void WriteRouteKey(string canonical, string key, Span<byte> buffer)
    {
        const byte colon = 58;
        const byte unitSeparator = 0x1F;
        var written = WriteNonNegativeIntUtf8(canonical.Length, buffer);
        buffer[written++] = colon;
        written += Encoding.UTF8.GetBytes(canonical, buffer[written..]);
        buffer[written++] = unitSeparator;
        _ = Encoding.UTF8.GetBytes(key, buffer[written..]);
    }

    private static ReadOnlySpan<byte> WriteVNodeKey(string node, int index, Span<byte> buffer)
    {
        const byte hash = 35;
        var written = Encoding.UTF8.GetBytes(node, buffer);
        buffer[written++] = hash;
        written += WriteNonNegativeIntUtf8(index, buffer[written..]);
        return buffer[..written];
    }

    private int FindFirstGreaterOrEqual(ulong hash)
    {
        var lo = 0;
        var hi = _ring.Length - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            if (_ring[mid].Hash < hash)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return lo == _ring.Length ? 0 : lo;
    }

    private string GetOwner(string cacheName, string key)
    {
        var hash = HashCacheRouteKey(cacheName, key);
        var idx = FindFirstGreaterOrEqual(hash);
        return _ring[idx].Node;
    }
}
