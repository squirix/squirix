using System;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Text;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

/// <summary>Cluster-owned DI registrations for static topology location (no child-namespace types).</summary>
internal static class RuntimeServiceRegistration
{
    /// <summary>Extension methods that register cluster locator services on <see cref="IServiceCollection" />.</summary>
    /// <param name="services">The service collection to register locators on.</param>
    extension(IServiceCollection services)
    {
        /// <summary>Registers static topology node location, ownership resolution, and the static leader table.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <remarks>
        /// The static leader table is added only when no table is registered yet; a node whose groups an election leads replaces it with the
        /// table of its election state afterwards.
        /// </remarks>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterLocator(TopologyOptions cluster)
        {
            var nodeIds = GetPeerNodeIds(cluster);
            _ = services.AddSingleton<INodeLocator>(new ConsistentHashRing(nodeIds, cluster.VirtualNodes));
            _ = services.AddSingleton(RingFingerprint.Create(cluster.ClusterId, nodeIds, cluster.VirtualNodes));
            _ = services.AddSingleton(static sp => new RingAgreement(sp.GetRequiredService<RingFingerprint>(), sp.GetRequiredService<ILogger<RingAgreement>>()));
            _ = services.AddSingleton<INodeOwnershipResolver>(static sp => new NodeOwnershipResolver(
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<TopologyOptions>()));
            services.TryAddSingleton<IGroupLeaderTable>(new StaticLeaderTable(cluster.NodeId));
            return services;
        }
    }

    /// <summary>Builds a consistent-hash <see cref="INodeLocator" /> for offline ring sampling tools.</summary>
    /// <param name="nodes">Distinct node identifiers.</param>
    /// <param name="virtualNodes">Virtual nodes per physical node.</param>
    /// <returns>A locator using the same ring algorithm as cluster hosting.</returns>
    [PublicAPI]
    internal static INodeLocator CreateHashLocator(ReadOnlySpan<string> nodes, int virtualNodes = 128) => new ConsistentHashRing(nodes, virtualNodes);

    private static string[] GetPeerNodeIds(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Count];

        for (var i = 0; i < peers.Count; i++)
            nodeIds[i] = peers[i].NodeId;

        return nodeIds;
    }

    /// <summary>
    /// Immutable consistent hashing ring with virtual nodes (vnodes), built from the cluster peer list at startup.
    /// Keys and virtual nodes are hashed with XXH64 (seed 0): fast, stable and well distributed; ring ownership does not need a cryptographic hash.
    /// </summary>
    [Immutable]
    private sealed class ConsistentHashRing : INodeLocator
    {
        /// <summary>ASCII &#39;:&#39;.</summary>
        private const byte RouteKeySeparator = 58;

        private const int StackHashBufferThreshold = 512;

        /// <summary>ASCII &#39;#&#39;.</summary>
        private const byte VNodeSeparator = 35;

        private readonly ImmutableArray<(ulong Hash, string Node)> _items;

        internal ConsistentHashRing(ReadOnlySpan<string> nodes, int virtualNodes)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualNodes);

            var distinct = DistinctNodeIds.InInsertionOrder(nodes);
            if (distinct.Length == 0)
                throw new ArgumentException("At least one node must be provided.", nameof(nodes));

            var ringSize = checked(distinct.Length * virtualNodes);
            var ring = new (ulong Hash, string Node)[ringSize];
            var index = 0;
            for (var nodeIndex = 0; nodeIndex < distinct.Length; nodeIndex++)
            {
                var node = distinct[nodeIndex];
                for (var i = 0; i < virtualNodes; i++)
                    ring[index++] = (HashVNode(node, i), node);
            }

            Array.Sort(ring, static (a, b) => a.Hash.CompareTo(b.Hash));
            _items = ImmutableCollectionsMarshal.AsImmutableArray(ring);
        }

        private static ReadOnlySpan<byte> DecimalDigitUtf8 => "0123456789"u8;

        public string GetOwner(string cacheName, string key)
        {
            ArgumentException.ThrowIfNullOrEmpty(cacheName);
            ArgumentNullException.ThrowIfNull(key);

            var kh = HashCacheRouteKey(cacheName, key);
            var idx = FindFirstGreaterOrEqual(kh);
            return _items[idx].Node;
        }

        private static ulong HashCacheRouteKey(string cacheName, string key)
        {
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

        private static ulong HashVNode(string node, int index)
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

        private static ulong Hash(ReadOnlySpan<byte> data) => XxHash64.Hash(data);

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
    }

    /// <summary>Cluster-backed node ownership resolver for inbound endpoint routing checks.</summary>
    [Immutable]
    private sealed class NodeOwnershipResolver : INodeOwnershipResolver
    {
        private readonly INodeLocator _locator;

        internal NodeOwnershipResolver(INodeLocator locator, TopologyOptions options)
        {
            // No options guard: the only caller feeds GetRequiredService (never null, throws first),
            // and an untestable guard would sink new-code coverage below the Sonar gate.
            ArgumentNullException.ThrowIfNull(locator);
            _locator = locator;
            SelfNodeId = options.NodeId;
        }

        /// <inheritdoc />
        public string SelfNodeId { get; }

        /// <inheritdoc />
        public string GetOwner(string cacheName, string key) => _locator.GetOwner(cacheName, key);
    }
}
