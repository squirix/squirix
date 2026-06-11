using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Validates inbound inter-node client certificates against the configured cluster trust root.
/// </summary>
internal static class ClusterMtlsClientCertificateValidator
{
    /// <summary>
    /// Validates a peer client certificate against the configured cluster CA.
    /// </summary>
    /// <param name="clientCertificate">The presented client certificate.</param>
    /// <param name="chain">Optional chain supplied by the TLS stack.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns><see langword="true" /> when the certificate chains to the cluster trust root and is time-valid.</returns>
    public static bool Validate(X509Certificate2? clientCertificate, X509Chain? chain, X509Certificate2 trustAnchor)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);

        if (clientCertificate is null)
            return false;

        try
        {
            using var validationChain = chain ?? new X509Chain();
            validationChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            validationChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            validationChain.ChainPolicy.CustomTrustStore.Clear();
            _ = validationChain.ChainPolicy.CustomTrustStore.Add(trustAnchor);
            return validationChain.Build(clientCertificate);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }
}
