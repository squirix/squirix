using System;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Builds caller certificate verifiers for tests that construct replication adapters directly.</summary>
internal static class ReplicationPeers
{
    /// <summary>Creates the verifier of a node, or none when its mTLS material is disabled.</summary>
    /// <param name="cluster">The cluster topology that names the remote peers.</param>
    /// <param name="material">The mTLS material of the node.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid.</param>
    /// <returns>The verifier, or <see langword="null" /> when the material is disabled.</returns>
    internal static VerifiedPeerCertificates? Create(TopologyOptions cluster, MtlsCertificate material, TimeProvider clock) =>
        VerifiedPeerCertificates.CreateIfEnabled(cluster, material, clock, NullLogger<VerifiedPeerCertificates>.Instance);
}
