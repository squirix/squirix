using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Loaded cluster mTLS certificate material for later transport wiring.
/// </summary>
internal sealed class ClusterMtlsCertificateMaterial : IDisposable
{
    private ClusterMtlsCertificateMaterial()
    {
        Enabled = false;
    }

    private ClusterMtlsCertificateMaterial(X509Certificate2 nodeCertificate, X509Certificate2 trustAnchor)
    {
        Enabled = true;
        NodeCertificate = nodeCertificate;
        TrustAnchor = trustAnchor;
    }

    /// <summary>
    /// Gets a disabled material instance with no loaded certificates.
    /// </summary>
    public static ClusterMtlsCertificateMaterial Disabled { get; } = new();

    /// <summary>
    /// Gets a value indicating whether cluster mTLS material was loaded.
    /// </summary>
    public bool Enabled { get; }

    /// <summary>
    /// Gets the local node certificate including its private key.
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global", Justification = "Read by inbound/outbound cluster mTLS transport wiring.")]
    internal X509Certificate2? NodeCertificate { get; }

    /// <summary>
    /// Gets the configured cluster trust root.
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global", Justification = "Read by inbound/outbound cluster mTLS transport wiring.")]
    internal X509Certificate2? TrustAnchor { get; }

    /// <summary>
    /// Loads node and trust-anchor certificates from validated options.
    /// </summary>
    /// <param name="options">Validated cluster mTLS options.</param>
    /// <param name="primaryListenPort">Primary external HTTPS listener port used to validate the internal listener port.</param>
    /// <param name="requiresInterNodeMtls">Whether inter-node mTLS is required for the configured cluster topology.</param>
    /// <returns>Loaded certificate material, or <see cref="Disabled"/> when inter-node mTLS is not required.</returns>
    public static ClusterMtlsCertificateMaterial Load(ClusterMtlsOptions options, int? primaryListenPort, bool requiresInterNodeMtls)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(primaryListenPort, requiresInterNodeMtls);

        if (!requiresInterNodeMtls)
            return Disabled;

        var trustAnchor = ClusterMtlsCertificateLoader.LoadTrustAnchor(options.CaPath!);
        var nodeCertificate = ClusterMtlsCertificateLoader.LoadNodeCertificate(options);
        ClusterMtlsCertificateLoader.EnsureNodeCertificateChainsToTrustAnchor(nodeCertificate, trustAnchor);
        return new ClusterMtlsCertificateMaterial(nodeCertificate, trustAnchor);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!Enabled)
            return;

        NodeCertificate?.Dispose();
        TrustAnchor?.Dispose();
    }
}
