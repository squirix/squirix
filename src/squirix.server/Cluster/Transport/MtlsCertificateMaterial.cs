using System;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Loaded cluster mTLS certificate material for later transport wiring.</summary>
internal sealed class MtlsCertificateMaterial : IDisposable
{
    private MtlsCertificateMaterial(X509Certificate2 nodeCertificate, X509Certificate2 trustAnchor)
    {
        Enabled = true;
        NodeCertificate = nodeCertificate;
        TrustAnchor = trustAnchor;
    }

    private MtlsCertificateMaterial()
    {
        Enabled = false;
    }

    /// <summary>Gets a value indicating whether cluster mTLS material was loaded.</summary>
    internal bool Enabled { get; }

    /// <summary>Gets the local node certificate including its private key.</summary>
    internal X509Certificate2? NodeCertificate { get; }

    /// <summary>Gets the configured cluster trust root.</summary>
    internal X509Certificate2? TrustAnchor { get; }

    /// <summary>Gets a disabled material instance with no loaded certificates.</summary>
    private static MtlsCertificateMaterial Disabled { get; } = new();

    /// <inheritdoc />
    void IDisposable.Dispose()
    {
        if (!Enabled)
            return;

        NodeCertificate?.Dispose();
        TrustAnchor?.Dispose();
    }

    /// <summary>Creates enabled certificate material from an already-loaded node certificate and trust anchor.</summary>
    /// <param name="nodeCertificate">The local node certificate including its private key.</param>
    /// <param name="trustAnchor">The configured cluster trust root.</param>
    /// <returns>Enabled certificate material.</returns>
    internal static MtlsCertificateMaterial Create(X509Certificate2 nodeCertificate, X509Certificate2 trustAnchor) => new(nodeCertificate, trustAnchor);

    /// <summary>Loads node and trust-anchor certificates from validated options.</summary>
    /// <param name="options">Validated cluster mTLS options.</param>
    /// <param name="primaryListenPort">Primary external HTTPS listener port used to validate the internal listener port.</param>
    /// <param name="requiresInterNodeMtls">Whether inter-node mTLS is required for the configured cluster topology.</param>
    /// <param name="localNodeId">Configured cluster node identifier; required when inter-node mTLS is enabled.</param>
    /// <returns>Loaded certificate material, or <see cref="Disabled" /> when inter-node mTLS is not required.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="options" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when inter-node mTLS is required but configuration or certificate material is invalid.</exception>
    internal static MtlsCertificateMaterial Load(MtlsOptions options, int? primaryListenPort, bool requiresInterNodeMtls, string? localNodeId = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(primaryListenPort, requiresInterNodeMtls);

        if (!requiresInterNodeMtls)
            return Disabled;

        if (string.IsNullOrWhiteSpace(localNodeId))
            throw new InvalidOperationException("Cluster NodeId is required to load inter-node mTLS certificate material.");

        var trustAnchor = MtlsCertificateLoader.LoadTrustAnchor(options.CaPath!);
        var nodeCertificate = MtlsCertificateLoader.LoadNodeCertificate(options);
        MtlsCertificateLoader.EnsureNodeCertificateChainsToTrustAnchor(nodeCertificate, trustAnchor);
        MtlsCertificateLoader.EnsureNodeCertificateMatchesNodeId(nodeCertificate, localNodeId);
        return Create(nodeCertificate, trustAnchor);
    }

    /// <summary>Loads cluster mTLS certificates from explicit file paths.</summary>
    private static class MtlsCertificateLoader
    {
        /// <summary>Ensures the node certificate chains to the configured cluster trust root.</summary>
        /// <param name="nodeCertificate">The node certificate.</param>
        /// <param name="trustAnchor">The configured cluster CA.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="nodeCertificate" /> or <paramref name="trustAnchor" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the node certificate is missing a private key or does not chain to the trust root.</exception>
        internal static void EnsureNodeCertificateChainsToTrustAnchor(X509Certificate2 nodeCertificate, X509Certificate2 trustAnchor)
        {
            ArgumentNullException.ThrowIfNull(nodeCertificate);
            ArgumentNullException.ThrowIfNull(trustAnchor);

            if (!nodeCertificate.HasPrivateKey)
                throw new InvalidOperationException("Cluster mTLS node certificate must include a private key.");

            using var chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            _ = chain.ChainPolicy.CustomTrustStore.Add(trustAnchor);

            if (chain.Build(nodeCertificate))
                return;

            var errorParts = new string[chain.ChainStatus.Length];
            for (var i = 0; i < chain.ChainStatus.Length; i++)
                errorParts[i] = chain.ChainStatus[i].StatusInformation.Trim();

            var errors = string.Join("; ", errorParts);
            var chainFailureMessage = string.IsNullOrWhiteSpace(errors) ? "mTLS node certificate does not chain to the configured trust root."
                : $"mTLS node certificate does not chain to the configured trust root. {errors}";
            throw new InvalidOperationException(chainFailureMessage);
        }

        /// <summary>Ensures the node certificate common name matches the configured cluster node identifier.</summary>
        /// <param name="nodeCertificate">The node certificate.</param>
        /// <param name="nodeId">Configured local cluster node identifier.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="nodeCertificate" /> is null.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="nodeId" /> is null or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the certificate identity does not match <paramref name="nodeId" />.</exception>
        internal static void EnsureNodeCertificateMatchesNodeId(X509Certificate2 nodeCertificate, string nodeId)
        {
            ArgumentNullException.ThrowIfNull(nodeCertificate);
            ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

            if (MtlsCertificateIdentity.MatchesNodeId(nodeCertificate, nodeId))
                return;

            var certificateNodeId = MtlsCertificateIdentity.TryGetNodeId(nodeCertificate, out var parsedNodeId) ? parsedNodeId : "<missing>";
            throw new InvalidOperationException($"Cluster mTLS node certificate identity '{certificateNodeId}' does not match configured NodeId '{nodeId}'.");
        }

        /// <summary>Loads the local node certificate and private key.</summary>
        /// <param name="options">Validated cluster mTLS options.</param>
        /// <returns>The loaded node certificate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="options" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the loaded certificate does not include a private key.</exception>
        internal static X509Certificate2 LoadNodeCertificate(MtlsOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (!string.IsNullOrWhiteSpace(options.CertPfxPath))
                return X509CertificateLoader.LoadPkcs12FromFile(options.CertPfxPath, options.CertPfxPassword, X509KeyStorageFlags.Exportable);

            var certificate = X509Certificate2.CreateFromPemFile(options.CertPath!, options.KeyPath);
            return certificate.HasPrivateKey ? certificate : throw new InvalidOperationException("Cluster mTLS node certificate must include a private key.");
        }

        /// <summary>Loads the cluster trust root certificate.</summary>
        /// <param name="caPath">Path to the PEM-encoded CA certificate.</param>
        /// <returns>The loaded trust anchor.</returns>
        internal static X509Certificate2 LoadTrustAnchor(string caPath) => X509CertificateLoader.LoadCertificateFromFile(caPath);
    }
}
