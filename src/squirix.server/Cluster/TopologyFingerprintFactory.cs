using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Cluster;

/// <summary>Builds canonical topology fingerprint inputs from cluster hosting options.</summary>
internal static class TopologyFingerprintFactory
{
    /// <summary>Computes the topology fingerprint for the configured cluster.</summary>
    /// <param name="topology">Cluster topology options.</param>
    /// <param name="mtlsOptions">Inter-node mTLS options used to derive effective peer URIs.</param>
    /// <returns>Canonical topology fingerprint.</returns>
    internal static TopologyFingerprint Compute(TopologyOptions topology, MtlsOptions mtlsOptions)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        return TopologyFingerprint.Compute(CreateInputs(topology, mtlsOptions));
    }

    /// <summary>Creates fingerprint inputs without hashing.</summary>
    /// <param name="topology">Cluster topology options.</param>
    /// <param name="mtlsOptions">Inter-node mTLS options used to derive effective peer URIs.</param>
    /// <returns>Fingerprint inputs.</returns>
    private static FingerprintInputs CreateInputs(TopologyOptions topology, MtlsOptions mtlsOptions)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(mtlsOptions);

        var peers = topology.Peers;
        var fingerprintPeers = new FingerprintPeer[peers.Length];
        var interNodeEnabled = MtlsTopology.RequiresInterNodeMtls(topology);
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = peers[i];
            var interNodeUri = ResolveInterNodeUri(peer, mtlsOptions, interNodeEnabled);
            fingerprintPeers[i] = new FingerprintPeer(peer.NodeId, peer.Uri.AbsoluteUri, interNodeUri.AbsoluteUri);
        }

        return new FingerprintInputs
        {
            ClusterId = topology.ClusterId,
            ConfigurationGeneration = topology.ConfigurationGeneration,
            ReplicaCount = topology.ReplicaCount,
            VirtualNodes = topology.VirtualNodes,
            Peers = fingerprintPeers,
            MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
            QuorumAckMode = PolicyOptions.QuorumAckMode,
        };
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
