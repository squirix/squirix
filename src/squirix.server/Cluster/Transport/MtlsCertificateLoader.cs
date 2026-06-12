using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Loads cluster mTLS certificates from explicit file paths.
/// </summary>
internal static class MtlsCertificateLoader
{
    /// <summary>
    /// Loads the cluster trust root certificate.
    /// </summary>
    /// <param name="caPath">Path to the PEM-encoded CA certificate.</param>
    /// <returns>The loaded trust anchor.</returns>
    public static X509Certificate2 LoadTrustAnchor(string caPath) => X509CertificateLoader.LoadCertificateFromFile(caPath);

    /// <summary>
    /// Loads the local node certificate and private key.
    /// </summary>
    /// <param name="options">Validated cluster mTLS options.</param>
    /// <returns>The loaded node certificate.</returns>
    public static X509Certificate2 LoadNodeCertificate(MtlsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!string.IsNullOrWhiteSpace(options.CertPfxPath))
        {
            return X509CertificateLoader.LoadPkcs12FromFile(options.CertPfxPath, options.CertPfxPassword, X509KeyStorageFlags.Exportable);
        }

        var certificate = X509Certificate2.CreateFromPemFile(options.CertPath!, options.KeyPath!);
        return certificate.HasPrivateKey ? certificate : throw new InvalidOperationException("Cluster mTLS node certificate must include a private key.");
    }

    /// <summary>
    /// Ensures the node certificate chains to the configured cluster trust root.
    /// </summary>
    /// <param name="nodeCertificate">The node certificate.</param>
    /// <param name="trustAnchor">The configured cluster CA.</param>
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

        var errors = string.Join("; ", chain.ChainStatus.Select(static status => status.StatusInformation.Trim()));
        var chainFailureMessage = string.IsNullOrWhiteSpace(errors)
            ? "mTLS node certificate does not chain to the configured trust root."
            : $"mTLS node certificate does not chain to the configured trust root. {errors}";
        throw new InvalidOperationException(chainFailureMessage);
    }
}
