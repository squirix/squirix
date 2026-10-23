using System;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>The SHA-256 digest of the raw certificate bytes.</summary>
/// <param name="Low">The first half of the digest.</param>
/// <param name="High">The second half of the digest.</param>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct CertificateDigest(UInt128 Low, UInt128 High)
{
    private const int DigestHalfSize = 16;
    private const int DigestSize = 32;

    /// <summary>Computes the digest of a certificate.</summary>
    /// <param name="certificate">The certificate to digest.</param>
    /// <returns>The digest of the raw certificate bytes.</returns>
    internal static CertificateDigest Of(X509Certificate2 certificate)
    {
        Span<byte> hash = stackalloc byte[DigestSize];
        _ = SHA256.HashData(certificate.RawDataMemory.Span, hash);
        return new CertificateDigest(MemoryMarshal.Read<UInt128>(hash), MemoryMarshal.Read<UInt128>(hash[DigestHalfSize..]));
    }
}
