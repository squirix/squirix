using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Standard TLS extensions for cluster test node certificates.</summary>
internal static class TestCertificateExtensions
{
    private static readonly X509KeyUsageExtension ClusterNodeKeyUsage = new(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true);

    private static readonly X509EnhancedKeyUsageExtension ClusterNodeEnhancedKeyUsage = new(CreateClusterNodeEnhancedKeyUsages(), false);

    /// <summary>Adds key usage and extended key usage required for mutual TLS client and server authentication.</summary>
    /// <param name="request">Certificate signing request.</param>
    internal static void AddClusterNodeExtensions(this CertificateRequest request)
    {
        request.CertificateExtensions.Add(ClusterNodeKeyUsage);
        request.CertificateExtensions.Add(ClusterNodeEnhancedKeyUsage);
    }

    private static OidCollection CreateClusterNodeEnhancedKeyUsages()
    {
        return
        [
            new Oid("1.3.6.1.5.5.7.3.1"),
            new Oid("1.3.6.1.5.5.7.3.2"),
        ];
    }
}
