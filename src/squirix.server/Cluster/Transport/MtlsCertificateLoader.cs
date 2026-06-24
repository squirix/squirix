using System;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Loads cluster mTLS certificates from explicit file paths.</summary>
internal static class MtlsCertificateLoader
{
    /// <summary>Ensures the node certificate chains to the configured cluster trust root.</summary>
    /// <param name="nodeCertificate">The node certificate.</param>
    /// <param name="trustAnchor">The configured cluster CA.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="nodeCertificate" /> or <paramref name="trustAnchor" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the node certificate is missing a private key or does not chain to the trust root.</exception>
    public static void EnsureNodeCertificateChainsToTrustAnchor(X509Certificate2 nodeCertificate, X509Certificate2 trustAnchor)
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
    public static void EnsureNodeCertificateMatchesNodeId(X509Certificate2 nodeCertificate, string nodeId)
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
    public static X509Certificate2 LoadNodeCertificate(MtlsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!string.IsNullOrWhiteSpace(options.CertPfxPath))
        {
            return X509CertificateLoader.LoadPkcs12FromFile(options.CertPfxPath, options.CertPfxPassword, X509KeyStorageFlags.Exportable);
        }

        var certificate = X509Certificate2.CreateFromPemFile(options.CertPath!, options.KeyPath);
        return certificate.HasPrivateKey ? certificate : throw new InvalidOperationException("Cluster mTLS node certificate must include a private key.");
    }

    /// <summary>Loads the cluster trust root certificate.</summary>
    /// <param name="caPath">Path to the PEM-encoded CA certificate.</param>
    /// <returns>The loaded trust anchor.</returns>
    public static X509Certificate2 LoadTrustAnchor(string caPath) => X509CertificateLoader.LoadCertificateFromFile(caPath);
}
