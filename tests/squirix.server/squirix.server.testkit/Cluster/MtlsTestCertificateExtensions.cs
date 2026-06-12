using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Standard TLS extensions for cluster test node certificates.
/// </summary>
internal static class MtlsTestCertificateExtensions
{
    /// <summary>
    /// Adds key usage and extended key usage required for mutual TLS client and server authentication.
    /// </summary>
    /// <param name="request">Certificate signing request.</param>
    public static void AddClusterNodeExtensions(this CertificateRequest request)
    {
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        var item = new X509EnhancedKeyUsageExtension(
            [
                new Oid("1.3.6.1.5.5.7.3.1"),
                new Oid("1.3.6.1.5.5.7.3.2"),
            ],
            false);
        request.CertificateExtensions.Add(item);
    }
}
