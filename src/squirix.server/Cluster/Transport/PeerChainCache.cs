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
/// trust state (for example a revoked intermediate) goes unseen. A refusal is remembered for the same age, in a table of its own with the same
/// capacity, so a hostile peer presenting many certificates can neither grow memory without limit nor evict a trusted entry; it is dropped as soon
/// as the certificate is trusted. A refusal whose verdict can change as time passes (a chain status that includes
/// <see cref="X509ChainStatusFlags.NotTimeValid" />), a chain that could not be built at all, and any failure that is not a plain chain or NodeId refusal
/// are never remembered, so a certificate that is not yet valid is accepted on the first handshake after it becomes valid. The chain is built outside the lock, so
/// concurrent misses for one certificate may each build it; the results are identical. The chain build also requires both TLS roles of a certificate
/// that names its extended key usage; that verdict depends on the certificate alone, so it is cached like the rest. The cache checks the chain only: the caller
/// checks the NodeId of the verdict on every use.
/// </remarks>
[ThreadSafe]
internal sealed class PeerChainCache
{
    /// <summary>The number of certificates remembered before the cache starts over.</summary>
    internal const int DefaultCapacity = 256;

    private const int DefaultMaxAgeMinutes = 10;

    private readonly int _capacity;
    private readonly Dictionary<CertificateDigest, Entry> _entries = [];
    private readonly Lock _gate = new();
    private readonly TimeSpan _maxAge;
    private readonly Dictionary<CertificateDigest, RefusedEntry> _refusals = [];
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

    /// <summary>Gets the number of remembered refusals.</summary>
    internal int RefusalCount
    {
        get
        {
            lock (_gate)
                return _refusals.Count;
        }
    }

    /// <summary>Checks that a peer certificate chains to the trust root and is valid at the given time, building the chain only when no fresh answer is remembered.</summary>
    /// <param name="certificate">The presented peer certificate.</param>
    /// <param name="now">The time the certificate is verified at.</param>
    /// <param name="verdict">The NodeId and validity window of the chain when the certificate is trusted.</param>
    /// <returns><see langword="true" /> when the certificate chains to the trust root, is valid at <paramref name="now" /> and names a node.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="certificate" /> is <see langword="null" />.</exception>
    internal bool TryVerify(X509Certificate2 certificate, DateTimeOffset now, out PeerChainVerdict verdict) => TryVerify(certificate, now, out verdict, out _);

    /// <summary>Checks that a peer certificate chains to the trust root and is valid at the given time, and says why it was refused when it is not.</summary>
    /// <param name="certificate">The presented peer certificate.</param>
    /// <param name="now">The time the certificate is verified at.</param>
    /// <param name="verdict">The NodeId and validity window of the chain when the certificate is trusted.</param>
    /// <param name="refusal">The reason the certificate was refused; the default value when it is trusted.</param>
    /// <returns><see langword="true" /> when the certificate chains to the trust root, is valid at <paramref name="now" /> and names a node.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="certificate" /> is <see langword="null" />.</exception>
    internal bool TryVerify(X509Certificate2 certificate, DateTimeOffset now, out PeerChainVerdict verdict, out PeerChainRefusal refusal)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        var key = CertificateDigest.Of(certificate);
        if (TryGetRemembered(in key, now, out verdict, out refusal))
            return refusal == default;

        _ = Interlocked.Increment(ref _chainBuilds);
        var chainTrusted = TryBuildTrustChain(certificate, now, out var notBefore, out var notAfter, out var chainStatus);
        if (!chainTrusted || !MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId))
        {
            verdict = default;
            refusal = new PeerChainRefusal(chainStatus, chainTrusted);
            RememberRefusal(in key, refusal, chainTrusted, now);
            return false;
        }

        refusal = default;
        verdict = new PeerChainVerdict(nodeId, notBefore, notAfter);
        RememberVerdict(in key, in verdict, now);
        return true;
    }

    /// <summary>Returns whether a refusal depends on the certificate and the trust root alone and not on the time, so it may be remembered.</summary>
    /// <param name="refusal">The refusal.</param>
    /// <param name="chainTrusted">Whether the chain was built and trusted, so the refusal is a missing NodeId.</param>
    /// <returns><see langword="true" /> for a missing NodeId or a chain failure without a time status.</returns>
    private static bool IsStableRefusal(PeerChainRefusal refusal, bool chainTrusted) =>
        chainTrusted || (refusal.ChainStatus != X509ChainStatusFlags.NoError && (refusal.ChainStatus & X509ChainStatusFlags.NotTimeValid) != X509ChainStatusFlags.NotTimeValid);

    /// <summary>Looks up a fresh remembered answer; a remembered refusal is never the default value, so the default refusal marks a trusted hit.</summary>
    /// <param name="key">The certificate digest.</param>
    /// <param name="now">The time the certificate is verified at.</param>
    /// <param name="verdict">The remembered verdict on a trusted hit.</param>
    /// <param name="refusal">The remembered refusal on a refused hit.</param>
    /// <returns><see langword="true" /> when a fresh trusted or refused answer is remembered.</returns>
    private bool TryGetRemembered(in CertificateDigest key, DateTimeOffset now, out PeerChainVerdict verdict, out PeerChainRefusal refusal)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry) && entry.Verdict.IsValidAt(now) && now - entry.VerifiedAt <= _maxAge)
            {
                verdict = entry.Verdict;
                refusal = default;
                return true;
            }

            if (_refusals.TryGetValue(key, out var refused) && now >= refused.RefusedAt && now - refused.RefusedAt <= _maxAge)
            {
                verdict = default;
                refusal = refused.Refusal;
                return true;
            }
        }

        verdict = default;
        refusal = default;
        return false;
    }

    private void RememberRefusal(in CertificateDigest key, PeerChainRefusal refusal, bool chainTrusted, DateTimeOffset now)
    {
        lock (_gate)
        {
            _ = _entries.Remove(key);
            if (!IsStableRefusal(refusal, chainTrusted))
            {
                _ = _refusals.Remove(key);
                return;
            }

            if (_refusals.Count >= _capacity && !_refusals.ContainsKey(key))
                _refusals.Clear();

            _refusals[key] = new RefusedEntry(refusal, now);
        }
    }

    private void RememberVerdict(in CertificateDigest key, in PeerChainVerdict verdict, DateTimeOffset now)
    {
        lock (_gate)
        {
            _ = _refusals.Remove(key);
            if (_entries.Count >= _capacity && !_entries.ContainsKey(key))
                _entries.Clear();

            _entries[key] = new Entry(verdict, now);
        }
    }

    private bool TryBuildTrustChain(
        X509Certificate2 certificate,
        DateTimeOffset verificationTime,
        out DateTimeOffset notBefore,
        out DateTimeOffset notAfter,
        out X509ChainStatusFlags chainStatus)
    {
        notBefore = DateTimeOffset.MinValue;
        notAfter = DateTimeOffset.MaxValue;
        chainStatus = X509ChainStatusFlags.NoError;
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
            {
                for (var i = 0; i < chain.ChainStatus.Length; i++)
                    chainStatus |= chain.ChainStatus[i].Status;

                return false;
            }

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

    /// <summary>A remembered verdict and the time its chain was built.</summary>
    /// <param name="Verdict">The remembered verdict.</param>
    /// <param name="VerifiedAt">The time the chain was built at.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Entry(PeerChainVerdict Verdict, DateTimeOffset VerifiedAt);

    /// <summary>A remembered refusal and the time it was decided.</summary>
    /// <param name="Refusal">The remembered refusal.</param>
    /// <param name="RefusedAt">The time the refusal was decided at.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct RefusedEntry(PeerChainRefusal Refusal, DateTimeOffset RefusedAt);
}
