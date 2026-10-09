using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class TopologyOptions
{
    /// <summary>The default bound of the dial of a client forward, before any TLS handshake: well below the per-attempt timeout of a forwarded call.</summary>
    internal static readonly TimeSpan DefaultInterNodeConnectTimeout = TimeSpan.FromMilliseconds(300);

    private readonly ServerPeer[] _peers;

    [SetsRequiredMembers]
    internal TopologyOptions(IReadOnlyList<ServerPeer> peers)
    {
        // Always snapshot into a private array so later mutation of the caller's collection (including
        // a caller-held ServerPeer[]) cannot change this instance and the [Immutable] guarantee holds.
        _peers = [.. peers];
    }

    [SetsRequiredMembers]
    internal TopologyOptions(ServerPeer peer)
    {
        _peers = [peer];
    }

    internal required string ClusterId { get; init; } = "cluster";

    /// <summary>Gets a value indicating whether automatic failover may trigger leader election (default false).</summary>
    internal bool AutomaticFailoverEnabled { get; init; }

    /// <summary>Gets a value indicating whether quorum reads require majority confirmation (default false).</summary>
    internal bool QuorumReadsEnabled { get; init; }

    /// <summary>Gets the configuration generation of the cluster topology (must be greater than zero).</summary>
    internal ulong ConfigurationGeneration { get; init; } = 1;

    /// <summary>Gets the longest time the dial of a client forward to a peer, before any TLS handshake, may take before it fails as a connect failure.</summary>
    internal TimeSpan InterNodeConnectTimeout { get; init; } = DefaultInterNodeConnectTimeout;

    internal required string NodeId { get; init; } = "node";

    internal IReadOnlyList<ServerPeer> Peers => _peers;

    /// <summary>Gets the configured replica factor including the original owner (default 1).</summary>
    internal int ReplicaCount { get; init; } = 1;

    /// <summary>Gets a value indicating whether the operator explicitly opted into RF&gt;1 replication (default false).</summary>
    internal bool ReplicationEnabled { get; init; }

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;
}
