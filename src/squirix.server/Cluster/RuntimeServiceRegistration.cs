using System;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

/// <summary>Cluster-owned DI registrations for static topology location (no child-namespace types).</summary>
internal static class RuntimeServiceRegistration
{
    /// <summary>
    /// Extension methods that register cluster locator services on <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The service collection to register locators on.</param>
    extension(IServiceCollection services)
    {
        /// <summary>Registers static topology node location and ownership resolution.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterLocator(TopologyOptions cluster)
        {
            _ = services.AddSingleton(new ConsistentHashNodeLocator(GetPeerNodeIds(cluster), cluster.VirtualNodes));
            _ = services.AddSingleton<INodeLocator>(static sp => sp.GetRequiredService<ConsistentHashNodeLocator>());
            _ = services.AddSingleton<INodeOwnershipResolver>(static sp => new NodeOwnershipResolver(
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<TopologyOptions>()));
            return services;
        }
    }

    /// <summary>Builds a consistent-hash <see cref="INodeLocator" /> for offline ring sampling tools.</summary>
    /// <param name="nodes">Distinct node identifiers.</param>
    /// <param name="virtualNodes">Virtual nodes per physical node.</param>
    /// <returns>A locator using the same ring algorithm as cluster hosting.</returns>
    [PublicAPI]
    internal static INodeLocator CreateHashLocator(ReadOnlySpan<string> nodes, int virtualNodes = 128) => new ConsistentHashNodeLocator(nodes, virtualNodes);

    private static string[] GetPeerNodeIds(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Length];

        for (var i = 0; i < peers.Length; i++)
            nodeIds[i] = peers[i].NodeId;

        return nodeIds;
    }

    /// <summary>
    /// Node locator backed by a <see cref="ConsistentHashRing" /> built from the cluster peer list at startup.
    /// </summary>
    [Immutable]
    private sealed class ConsistentHashNodeLocator : INodeLocator
    {
        private readonly ConsistentHashRing _ring;

        internal ConsistentHashNodeLocator(ReadOnlySpan<string> nodes, int virtualNodes = 128)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

            _ring = new ConsistentHashRing(nodes, virtualNodes);
        }

        public string GetOwner(string cacheName, string key) => _ring.GetOwner(cacheName, key);

        /// <summary>Immutable consistent hashing ring with virtual nodes (vnodes).</summary>
        [Immutable]
        private sealed class ConsistentHashRing : INodeLocator
        {
            private readonly IHash _hash;
            private readonly ImmutableArray<(ulong Hash, string Node)> _items;

            internal ConsistentHashRing(ReadOnlySpan<string> nodes, int virtualNodes = 128, IHash? hash = null)
            {
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

                _hash = hash ?? new Sha256Hasher();

                var distinct = CollectDistinctNodes(nodes);
                if (distinct.Length == 0)
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

            private static string[] CollectDistinctNodes(ReadOnlySpan<string> nodes) => DistinctNodeIds.InInsertionOrder(nodes);

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
            [Immutable]
            private sealed class Sha256Hasher : IHash
            {
                /// <summary>
                /// ASCII &#39;:&#39;.
                /// </summary>
                private const byte RouteKeySeparator = 58;

                private const int StackHashBufferThreshold = 512;

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
                    if (byteCount <= StackHashBufferThreshold)
                    {
                        Span<byte> buffer = stackalloc byte[byteCount];
                        return Hash(WriteCacheRouteKey(cacheName, key, buffer));
                    }

                    var rented = ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        return Hash(WriteCacheRouteKey(cacheName, key, rented.AsSpan(0, byteCount)));
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
                    if (byteCount <= StackHashBufferThreshold)
                    {
                        Span<byte> buffer = stackalloc byte[byteCount];
                        return Hash(WriteVNodeKey(node, index, buffer));
                    }

                    var rented = ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        return Hash(WriteVNodeKey(node, index, rented.AsSpan(0, byteCount)));
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

                private static ReadOnlySpan<byte> WriteCacheRouteKey(string cacheName, string key, Span<byte> buffer)
                {
                    var written = WriteNonNegativeIntUtf8(cacheName.Length, buffer);
                    buffer[written++] = RouteKeySeparator;
                    written += Encoding.UTF8.GetBytes(cacheName.AsSpan(), buffer[written..]);
                    buffer[written++] = 0x1F;
                    written += Encoding.UTF8.GetBytes(key.AsSpan(), buffer[written..]);
                    return buffer[..written];
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

                private static ReadOnlySpan<byte> WriteVNodeKey(string node, int index, Span<byte> buffer)
                {
                    var written = Encoding.UTF8.GetBytes(node.AsSpan(), buffer);
                    buffer[written++] = VNodeSeparator;
                    written += WriteNonNegativeIntUtf8(index, buffer[written..]);
                    return buffer[..written];
                }
            }
        }
    }

    /// <summary>Cluster-backed node ownership resolver for inbound endpoint routing checks.</summary>
    [Immutable]
    private sealed class NodeOwnershipResolver : INodeOwnershipResolver
    {
        private readonly INodeLocator _locator;

        internal NodeOwnershipResolver(INodeLocator locator, TopologyOptions topologyOptions)
        {
            ArgumentNullException.ThrowIfNull(locator);
            _locator = locator;
            SelfNodeId = topologyOptions?.NodeId ?? ThrowHelper.Throw<string>(new ArgumentNullException(nameof(topologyOptions)));
        }

        /// <inheritdoc />
        public string SelfNodeId { get; }

        /// <inheritdoc />
        public string GetOwner(string cacheName, string key) => _locator.GetOwner(cacheName, key);
    }
}
