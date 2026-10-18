using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Remembers peer certificates whose chain to one cluster trust root was built successfully, so repeated handshakes skip the chain build.</summary>
/// <remarks>
/// The trust root is fixed per instance, so a certificate of another authority never gets a hit. An entry answers only inside the validity
/// window of its chain and for at most the configured age; otherwise the chain is rebuilt, which bounds how long a change of operating system
/// trust state (for example a revoked intermediate) goes unseen. Failures are never cached. The chain is built outside the lock, so
/// concurrent misses for one certificate may each build it; the results are identical. The chain build also requires both TLS roles of a certificate
/// that names its extended key usage; that verdict depends on the certificate alone, so it is cached like the rest. The cache checks the chain only: the caller
/// checks the NodeId of the verdict on every use.
/// </remarks>
[ThreadSafe]
internal sealed class PeerChainCache
{
    /// <summary>The number of certificates remembered before the cache starts over.</summary>
    internal const int DefaultCapacity = 256;

    private const int DigestHalfSize = 16;
    private const int DigestSize = 32;
    private const int DefaultMaxAgeMinutes = 10;

    private readonly int _capacity;
    private readonly Dictionary<CertificateDigest, Entry> _entries = [];
    private readonly Lock _gate = new();
    private readonly TimeSpan _maxAge;
    private readonly X509Certificate2 _trustAnchor;
    private long _chainBuilds;

    /// <summary>Initializes a new instance of the <see cref="PeerChainCache" /> class.</summary>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="capacity">The number of certificates remembered before the cache starts over.</param>
    /// <param name="maxAge">The longest time a successful chain build is trusted without a rebuild.</param>
    /// <exception cref="ArgumentNullException"><paramref name="trustAnchor" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity" /> is not positive or <paramref name="maxAge" /> is negative.</exception>
    internal PeerChainCache(X509Certificate2 trustAnchor, int capacity, TimeSpan maxAge)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAge, TimeSpan.Zero);
        _trustAnchor = trustAnchor;
        _capacity = capacity;
        _maxAge = maxAge;
    }

    /// <summary>Gets the longest time a successful chain build is trusted without a rebuild, by default.</summary>
    internal static TimeSpan DefaultMaxAge => TimeSpan.FromMinutes(DefaultMaxAgeMinutes);

    /// <summary>Gets the number of certificate chains built so far.</summary>
    internal long ChainBuilds => Interlocked.Read(ref _chainBuilds);

    /// <summary>Gets the number of remembered certificates.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
                return _entries.Count;
        }
    }

    /// <summary>Checks that a peer certificate chains to the trust root and is valid at the given time, building the chain only when no fresh answer is remembered.</summary>
    /// <param name="certificate">The presented peer certificate.</param>
    /// <param name="now">The time the certificate is verified at.</param>
    /// <param name="verdict">The NodeId and validity window of the chain when the certificate is trusted.</param>
    /// <returns><see langword="true" /> when the certificate chains to the trust root, is valid at <paramref name="now" /> and names a node.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="certificate" /> is <see langword="null" />.</exception>
    internal bool TryVerify(X509Certificate2 certificate, DateTimeOffset now, out PeerChainVerdict verdict)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        var key = CertificateDigest.Of(certificate);
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry) && entry.Verdict.IsValidAt(now) && now - entry.VerifiedAt <= _maxAge)
            {
                verdict = entry.Verdict;
                return true;
            }
        }

        _ = Interlocked.Increment(ref _chainBuilds);
        if (!TryBuildTrustChain(certificate, now, out var notBefore, out var notAfter) || !MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId))
        {
            lock (_gate)
                _ = _entries.Remove(key);

            verdict = default;
            return false;
        }

        verdict = new PeerChainVerdict(nodeId, notBefore, notAfter);
        lock (_gate)
        {
            if (_entries.Count >= _capacity && !_entries.ContainsKey(key))
                _entries.Clear();

            _entries[key] = new Entry(verdict, now);
        }

        return true;
    }

    private bool TryBuildTrustChain(X509Certificate2 certificate, DateTimeOffset verificationTime, out DateTimeOffset notBefore, out DateTimeOffset notAfter)
    {
        notBefore = DateTimeOffset.MinValue;
        notAfter = DateTimeOffset.MaxValue;
        try
        {
            using var chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.DisableCertificateDownloads = true;
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.VerificationTime = verificationTime.UtcDateTime;
            MtlsCertificateIdentity.RequireNodeRoles(chain.ChainPolicy);
            chain.ChainPolicy.CustomTrustStore.Clear();
            _ = chain.ChainPolicy.CustomTrustStore.Add(_trustAnchor);
            if (!chain.Build(certificate))
                return false;

            foreach (var element in chain.ChainElements)
            {
                var start = new DateTimeOffset(element.Certificate.NotBefore.ToUniversalTime());
                var end = new DateTimeOffset(element.Certificate.NotAfter.ToUniversalTime());
                notBefore = start > notBefore ? start : notBefore;
                notAfter = end < notAfter ? end : notAfter;
            }

            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    /// <summary>The SHA-256 digest of the raw certificate bytes.</summary>
    /// <param name="Low">The first half of the digest.</param>
    /// <param name="High">The second half of the digest.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct CertificateDigest(UInt128 Low, UInt128 High)
    {
        internal static CertificateDigest Of(X509Certificate2 certificate)
        {
            Span<byte> hash = stackalloc byte[DigestSize];
            _ = SHA256.HashData(certificate.RawDataMemory.Span, hash);
            return new CertificateDigest(MemoryMarshal.Read<UInt128>(hash), MemoryMarshal.Read<UInt128>(hash[DigestHalfSize..]));
        }
    }

    /// <summary>A remembered verdict and the time its chain was built.</summary>
    /// <param name="Verdict">The remembered verdict.</param>
    /// <param name="VerifiedAt">The time the chain was built at.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Entry(PeerChainVerdict Verdict, DateTimeOffset VerifiedAt);
}
