using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster;

/// <summary>
/// Node locator backed by a <see cref="ConsistentHashRing" /> built from the cluster peer list at startup.
/// </summary>
internal sealed class ConsistentHashNodeLocator : INodeLocator
{
    private readonly ConsistentHashRing _ring;

    public ConsistentHashNodeLocator(ReadOnlySpan<string> nodes, int virtualNodes = 128)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

        _ring = new ConsistentHashRing(nodes, virtualNodes);
    }

    public string GetOwner(string cacheName, string key) => _ring.GetOwner(cacheName, key);

    /// <summary>Immutable consistent hashing ring with virtual nodes (vnodes).</summary>
    private sealed class ConsistentHashRing : INodeLocator
    {
        private readonly IHash _hash;
        private readonly ImmutableArray<(ulong Hash, string Node)> _items;

        public ConsistentHashRing(ReadOnlySpan<string> nodes, int virtualNodes = 128, IHash? hash = null)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

            _hash = hash ?? new Sha256Hasher();

            var distinct = CollectDistinctNodes(nodes);
            if (distinct.Length is 0)
                throw new ArgumentException("At least one node must be provided.", nameof(nodes));

            var ringSize = checked(distinct.Length * virtualNodes);
            var ring = new (ulong Hash, string Node)[ringSize];
            var index = 0;
            for (var nodeIndex = 0; nodeIndex < distinct.Length; nodeIndex++)
            {
                var node = distinct[nodeIndex];
                for (var i = 0; i < virtualNodes; i++)
                    ring[index++] = (_hash.HashVNode(node, i), node);
            }

            Array.Sort(ring, static (a, b) => a.Hash.CompareTo(b.Hash));
            _items = ImmutableCollectionsMarshal.AsImmutableArray(ring);
        }

        public string GetOwner(string cacheName, string key)
        {
            ArgumentException.ThrowIfNullOrEmpty(cacheName);
            ArgumentNullException.ThrowIfNull(key);

            var kh = _hash.HashCacheRouteKey(cacheName, key);
            var idx = FindFirstGreaterOrEqual(kh);
            return _items[idx].Node;
        }

        private static string[] CollectDistinctNodes(ReadOnlySpan<string> nodes)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            string[] buffer = [];
            var writeIndex = 0;

            for (var i = 0; i < nodes.Length; i++)
            {
                var value = nodes[i];
                if (string.IsNullOrWhiteSpace(value) || !seen.Add(value))
                    continue;

                if (writeIndex == buffer.Length)
                    buffer = GrowDistinctBuffer(buffer, writeIndex);

                buffer[writeIndex++] = value;
            }

            return TrimDistinctBuffer(buffer, writeIndex);
        }

        private static string[] GrowDistinctBuffer(string[] buffer, int writeIndex)
        {
            var grown = new string[buffer.Length is 0 ? 4 : buffer.Length * 2];
            buffer.AsSpan(0, writeIndex).CopyTo(grown);
            return grown;
        }

        private static string[] TrimDistinctBuffer(string[] buffer, int writeIndex)
        {
            if (writeIndex is 0)
                return [];

            if (writeIndex == buffer.Length)
                return buffer;

            var result = new string[writeIndex];
            buffer.AsSpan(0, writeIndex).CopyTo(result);
            return result;
        }

        private int FindFirstGreaterOrEqual(ulong hash)
        {
            int lo = 0, hi = _items.Length - 1;
            while (lo <= hi)
            {
                var mid = (lo + hi) >> 1;
                var midHash = _items[mid].Hash;
                if (midHash < hash)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }

            return lo == _items.Length ? 0 : lo;
        }

        /// <summary>
        /// SHA-256 with 64-bit truncation (first 8 bytes as little-endian).
        /// Very even distribution for CH rings; slower than FNV, but OK for ring build/lookups.
        /// </summary>
        private sealed class Sha256Hasher : IHash
        {
            /// <summary>
            /// ASCII &#39;:&#39;.
            /// </summary>
            private const byte RouteKeySeparator = 58;

            /// <summary>
            /// ASCII &#39;#&#39;.
            /// </summary>
            private const byte VNodeSeparator = 35;

            private static ReadOnlySpan<byte> DecimalDigitUtf8 => "0123456789"u8;

            public ulong HashCacheRouteKey(string cacheName, string key)
            {
                ArgumentNullException.ThrowIfNull(cacheName);
                ArgumentNullException.ThrowIfNull(key);

                var byteCount = checked(CountDigits(cacheName.Length) + 1 + Encoding.UTF8.GetByteCount(cacheName) + 1 + Encoding.UTF8.GetByteCount(key));
                var rented = ArrayPool<byte>.Shared.Rent(byteCount);

                try
                {
                    var buffer = rented.AsSpan(0, byteCount);
                    var written = WriteNonNegativeIntUtf8(cacheName.Length, buffer);
                    buffer[written++] = RouteKeySeparator;
                    written += Encoding.UTF8.GetBytes(cacheName.AsSpan(), buffer[written..]);
                    buffer[written++] = 0x1F;
                    written += Encoding.UTF8.GetBytes(key.AsSpan(), buffer[written..]);
                    return Hash(buffer[..written]);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }

            public ulong HashVNode(string node, int index)
            {
                ArgumentNullException.ThrowIfNull(node);
                ArgumentOutOfRangeException.ThrowIfNegative(index);

                var byteCount = checked(Encoding.UTF8.GetByteCount(node) + 1 + CountDigits(index));
                var rented = ArrayPool<byte>.Shared.Rent(byteCount);

                try
                {
                    var buffer = rented.AsSpan(0, byteCount);
                    var written = Encoding.UTF8.GetBytes(node.AsSpan(), buffer);
                    buffer[written++] = VNodeSeparator;
                    written += WriteNonNegativeIntUtf8(index, buffer[written..]);
                    return Hash(buffer[..written]);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
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

            private static ulong Hash(ReadOnlySpan<byte> data)
            {
                Span<byte> digest = stackalloc byte[32];
                _ = SHA256.HashData(data, digest);
                return BitConverter.ToUInt64(digest);
            }

            private static int WriteNonNegativeIntUtf8(int value, Span<byte> destination)
            {
                var digits = CountDigits(value);
                for (var i = digits - 1; i >= 0; i--)
                {
                    destination[i] = DecimalDigitUtf8[value % 10];
                    value /= 10;
                }

                return digits;
            }
        }
    }
}
