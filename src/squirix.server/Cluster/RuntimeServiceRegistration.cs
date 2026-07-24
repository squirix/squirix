using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster;

/// <summary>Cluster-owned DI registrations for static topology transport and inter-node client pooling.</summary>
internal static class RuntimeServiceRegistration
{
    /// <summary>
    /// Extension methods that register cluster runtime services on <see cref="IServiceCollection" />.
    /// </summary>
    extension(IServiceCollection services)
    {
        /// <summary>Registers static topology node location, gRPC client pool, and shared cluster-side singletons used by the node host.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="callPolicyFactory">Optional per-endpoint call policy factory; defaults to a conservative remote policy.</param>
        /// <param name="peerHandlerFactory">Optional per-peer HTTP handler factory for pooled gRPC channels.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixClusterServices(
            TopologyOptions cluster,
            Func<string, ServerCallPolicy>? callPolicyFactory,
            Func<string, HttpMessageHandler>? peerHandlerFactory)
        {
            RegisterLocatorAndOwnership(services, cluster);
            RegisterInterceptors(services, cluster);
            RegisterClientPool(services, cluster, callPolicyFactory, peerHandlerFactory);
            return services;
        }
    }

    /// <summary>Builds a consistent-hash <see cref="INodeLocator" /> for offline ring sampling tools.</summary>
    /// <param name="nodes">Distinct node identifiers.</param>
    /// <param name="virtualNodes">Virtual nodes per physical node.</param>
    /// <returns>A locator using the same ring algorithm as cluster hosting.</returns>
    [PublicAPI]
    internal static INodeLocator CreateHashLocator(ReadOnlySpan<string> nodes, int virtualNodes = 128) => new ConsistentHashNodeLocator(nodes, virtualNodes);

    private static ServerPeer[] CopyPeers(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var copy = new ServerPeer[peers.Length];

        for (var i = 0; i < peers.Length; i++)
            copy[i] = peers[i];

        return copy;
    }

    private static string[] GetPeerNodeIds(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Length];

        for (var i = 0; i < peers.Length; i++)
            nodeIds[i] = peers[i].NodeId;

        return nodeIds;
    }

    private static void RegisterClientPool(
        IServiceCollection services,
        TopologyOptions cluster,
        Func<string, ServerCallPolicy>? callPolicyFactory,
        Func<string, HttpMessageHandler>? peerHandlerFactory)
    {
        _ = services.AddSingleton<IServerClientPool>(sp =>
        {
            var material = sp.GetRequiredService<MtlsCertificateMaterial>();
            var mtlsOptions = sp.GetRequiredService<MtlsOptions>();
            var interNodeMtlsEnabled = material.Enabled;
            return new ServerClientPool(
                CopyPeers(cluster),
                new ServerClientPoolArgs
                {
                    PolicyFactory = callPolicyFactory ?? (static _ => new ServerCallPolicy(
                        TimeSpan.FromSeconds(3),
                        3,
                        TimeSpan.FromMilliseconds(60),
                        TimeSpan.FromMilliseconds(600))),
                    PeerHandlerFactory = peerHandlerFactory,
                    Interceptor = sp.GetRequiredService<ClientInterceptor>(),
                    MtlsOptions = mtlsOptions,
                    MtlsMaterial = material,
                    InterNodeMtlsEnabled = interNodeMtlsEnabled,
                    InternalOwnerInterceptor = interNodeMtlsEnabled ? sp.GetRequiredService<ClusterInternalOwnerClientInterceptor>() : null,
                });
        });
    }

    private static void RegisterInterceptors(IServiceCollection services, TopologyOptions cluster)
    {
        _ = services.AddSingleton(sp => new ClientInterceptor(sp.GetRequiredService<ILogger<ClientInterceptor>>(), cluster.NodeId));
        _ = services.AddSingleton(sp => new ServerInterceptor(sp.GetRequiredService<ILogger<ServerInterceptor>>(), cluster.NodeId));
        _ = services.AddSingleton<ClusterInternalOwnerClientInterceptor>();
    }

    private static void RegisterLocatorAndOwnership(IServiceCollection services, TopologyOptions cluster)
    {
        _ = services.AddSingleton(new ConsistentHashNodeLocator(GetPeerNodeIds(cluster), cluster.VirtualNodes));
        _ = services.AddSingleton<INodeLocator>(static sp => sp.GetRequiredService<ConsistentHashNodeLocator>());
        _ = services.AddSingleton<INodeOwnershipResolver>(static sp => new NodeOwnershipResolver(sp.GetRequiredService<INodeLocator>(), sp.GetRequiredService<TopologyOptions>()));
    }

    /// <summary>
    /// Node locator backed by a <see cref="ConsistentHashRing" /> built from the cluster peer list at startup.
    /// </summary>
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
        private sealed class ConsistentHashRing : INodeLocator
        {
            private readonly IHash _hash;
            private readonly ImmutableArray<(ulong Hash, string Node)> _items;

            internal ConsistentHashRing(ReadOnlySpan<string> nodes, int virtualNodes = 128, IHash? hash = null)
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

    /// <summary>Cluster-backed node ownership resolver for inbound endpoint routing checks.</summary>
    private sealed class NodeOwnershipResolver : INodeOwnershipResolver
    {
        private readonly INodeLocator _locator;

        internal NodeOwnershipResolver(INodeLocator locator, TopologyOptions topologyOptions)
        {
            _locator = locator ?? throw new ArgumentNullException(nameof(locator));
            SelfNodeId = topologyOptions.NodeId ?? throw new ArgumentNullException(nameof(topologyOptions));
        }

        /// <inheritdoc />
        public string SelfNodeId { get; }

        /// <inheritdoc />
        public string GetOwner(string cacheName, string key) => _locator.GetOwner(cacheName, key);
    }
}
