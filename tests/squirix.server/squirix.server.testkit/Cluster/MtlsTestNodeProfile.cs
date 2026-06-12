namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Selects deterministic inter-node mTLS startup behavior for a test node.
/// </summary>
public enum MtlsTestNodeProfile
{
    /// <summary>
    /// Normal trusted cluster mTLS for inbound and outbound inter-node transport.
    /// </summary>
    Normal = 0,

    /// <summary>
    /// Outbound cluster calls trust the cluster CA but do not present a client certificate.
    /// </summary>
    NoOutboundClientCertificate = 1,

    /// <summary>
    /// Outbound cluster calls present a client certificate signed by an untrusted test CA.
    /// </summary>
    UntrustedOutboundClientCertificate = 2,

    /// <summary>
    /// The internal mTLS listener presents a server certificate signed by an untrusted test CA.
    /// </summary>
    UntrustedInboundServerCertificate = 3,

    /// <summary>
    /// The node certificate is expired but still chains to the cluster test CA.
    /// </summary>
    ExpiredPeerCertificate = 4,
}
