using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Cluster;

internal sealed class TopologyOptions
{
    [SetsRequiredMembers]
    internal TopologyOptions(ServerPeer[] peers)
    {
        Peers = peers;
    }

    [SetsRequiredMembers]
    internal TopologyOptions(ServerPeer peer)
    {
        Peers = [peer];
    }

    internal required string ClusterId { get; init; } = "cluster";

    /// <summary>Gets the stopped-topology configuration generation (must be greater than zero).</summary>
    internal ulong ConfigurationGeneration { get; init; } = 1;

    internal required string NodeId { get; init; } = "node";

    internal ServerPeer[] Peers { get; }

    /// <summary>Gets the configured replica factor including the original owner (default 1).</summary>
    internal int ReplicaCount { get; init; } = 1;

    internal required Uri Uri { get; init; } = new("https://localhost:6001");

    internal int VirtualNodes { get; init; } = 128;

    /// <summary>Computes the canonical topology fingerprint for this configuration.</summary>
    /// <param name="mtlsOptions">Inter-node mTLS options used to derive effective peer URIs.</param>
    /// <returns>Canonical topology fingerprint.</returns>
    internal TopologyFingerprint CreateFingerprint(MtlsOptions mtlsOptions)
    {
        ArgumentNullException.ThrowIfNull(mtlsOptions);

        var peers = Peers;
        var fingerprintPeers = new FingerprintPeer[peers.Length];
        var interNodeEnabled = MtlsTopology.RequiresInterNodeMtls(this);
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = peers[i];
            var interNodeUri = ResolveInterNodeUri(peer, mtlsOptions, interNodeEnabled);
            fingerprintPeers[i] = new FingerprintPeer(peer.NodeId, peer.Uri, interNodeUri);
        }

        return TopologyFingerprint.Compute(
            new FingerprintInputs
            {
                ClusterId = ClusterId,
                ConfigurationGeneration = ConfigurationGeneration,
                ReplicaCount = ReplicaCount,
                VirtualNodes = VirtualNodes,
                Peers = fingerprintPeers,
                MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
                QuorumAckMode = PolicyOptions.QuorumAckMode,
            });
    }

    private static Uri ResolveInterNodeUri(ServerPeer peer, MtlsOptions mtlsOptions, bool interNodeEnabled)
    {
        if (!interNodeEnabled)
            return peer.Uri;

        if (peer.InterNodeUri is { } configured)
            return configured;

        if (mtlsOptions.InternalListenPort <= 0)
            return peer.Uri;

        return new UriBuilder(peer.Uri.Scheme, peer.Uri.Host, mtlsOptions.InternalListenPort).Uri;
    }
}
