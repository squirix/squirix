using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Decides the peer certificate check of an internode mTLS handshake and logs each refusal once per certificate and reason.</summary>
/// <remarks>
/// A peer that keeps retrying with a refused certificate would flood the log, so a refusal is logged when its certificate, reason and
/// expected peer were not logged yet, again after the relog interval, and again when the clock steps back. The table of logged refusals
/// starts over when it is full. Logging never changes the verdict. A refusal is decided under the lock; the message is formatted and written outside it.
/// </remarks>
[ThreadSafe]
internal sealed class PeerHandshakeVerifier
{
    /// <summary>The number of refusals remembered before the table starts over.</summary>
    internal const int DefaultCapacity = 256;

    private const int DefaultRelogHours = 1;
    private const string TimestampFormat = "O";
    private const string UsageHint = " (an extended key usage must list both serverAuth and clientAuth)";

    private readonly int _capacity;
    private readonly PeerChainCache _chains;
    private readonly TimeProvider _clock;
    private readonly Lock _gate = new();
    private readonly Dictionary<RefusalKey, DateTimeOffset> _logged = [];
    private readonly ILogger _logger;
    private readonly TimeSpan _relogInterval;

    /// <summary>Initializes a new instance of the <see cref="PeerHandshakeVerifier" /> class with the default capacity and relog interval.</summary>
    /// <param name="chains">The cache of certificate chains built against the cluster trust root.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid and when a refusal is logged again.</param>
    /// <param name="logger">The logger refusals are written to.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal PeerHandshakeVerifier(PeerChainCache chains, TimeProvider clock, ILogger logger)
        : this(chains, clock, logger, DefaultCapacity, DefaultRelogInterval)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PeerHandshakeVerifier" /> class.</summary>
    /// <param name="chains">The cache of certificate chains built against the cluster trust root.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid and when a refusal is logged again.</param>
    /// <param name="logger">The logger refusals are written to.</param>
    /// <param name="capacity">The number of refusals remembered before the table starts over.</param>
    /// <param name="relogInterval">The time after which a repeated refusal is logged again.</param>
    /// <exception cref="ArgumentNullException">A reference argument is <see langword="null" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity" /> is not positive or <paramref name="relogInterval" /> is negative.</exception>
    internal PeerHandshakeVerifier(PeerChainCache chains, TimeProvider clock, ILogger logger, int capacity, TimeSpan relogInterval)
    {
        ArgumentNullException.ThrowIfNull(chains);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(capacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(relogInterval, TimeSpan.Zero);
        _chains = chains;
        _clock = clock;
        _logger = logger;
        _capacity = capacity;
        _relogInterval = relogInterval;
    }

    /// <summary>Why a peer certificate was refused.</summary>
    private enum RefusalReason
    {
        /// <summary>The chain to the cluster trust root failed or could not be built.</summary>
        UntrustedChain = 0,

        /// <summary>The chain is trusted but the certificate names no NodeId.</summary>
        MissingNodeId = 1,

        /// <summary>The certificate names a NodeId that is not a configured peer.</summary>
        NotConfiguredPeer = 2,

        /// <summary>The certificate names a NodeId other than the expected peer.</summary>
        NotExpectedPeer = 3,
    }

    /// <summary>Gets the time after which a repeated refusal is logged again, by default.</summary>
    internal static TimeSpan DefaultRelogInterval => TimeSpan.FromHours(DefaultRelogHours);

    /// <summary>Checks the certificate of an inbound peer connection: it must chain to the trust root and name a configured peer.</summary>
    /// <param name="certificate">The client certificate of the connection.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and names a configured peer.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal bool VerifyConfiguredPeer(X509Certificate2 certificate, string[] nodeIds)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(nodeIds);
        var now = _clock.GetUtcNow();
        if (!_chains.TryVerify(certificate, now, out var verdict, out var refusal))
        {
            Refuse(certificate, ChainReason(refusal), refusal.ChainStatus, null, string.Empty, now);
            return false;
        }

        if (MtlsTopology.IsRemotePeer(nodeIds, verdict.NodeId))
            return true;

        Refuse(certificate, RefusalReason.NotConfiguredPeer, X509ChainStatusFlags.NoError, null, verdict.NodeId, now);
        return false;
    }

    /// <summary>Checks the certificate presented by the peer of an outbound connection: it must chain to the trust root and name the expected peer.</summary>
    /// <param name="certificate">The server certificate of the connection; only read during the call.</param>
    /// <param name="expectedPeerNodeId">The NodeId of the peer the connection dials.</param>
    /// <returns><see langword="true" /> when the certificate is trusted and names the expected peer.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal bool VerifyExpectedPeer(X509Certificate2 certificate, string expectedPeerNodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(expectedPeerNodeId);
        var now = _clock.GetUtcNow();
        if (!_chains.TryVerify(certificate, now, out var verdict, out var refusal))
        {
            Refuse(certificate, ChainReason(refusal), refusal.ChainStatus, expectedPeerNodeId, string.Empty, now);
            return false;
        }

        if (string.Equals(verdict.NodeId, expectedPeerNodeId, StringComparison.Ordinal))
            return true;

        Refuse(certificate, RefusalReason.NotExpectedPeer, X509ChainStatusFlags.NoError, expectedPeerNodeId, verdict.NodeId, now);
        return false;
    }

    private static RefusalReason ChainReason(PeerChainRefusal refusal) => refusal.MissingNodeId ? RefusalReason.MissingNodeId : RefusalReason.UntrustedChain;

    private static string FormatChainReason(X509Certificate2 certificate, X509ChainStatusFlags chainStatus)
    {
        var text = chainStatus == X509ChainStatusFlags.NoError
            ? "its chain to the cluster trust root could not be built"
            : $"its chain to the cluster trust root failed with {chainStatus}";
        if (chainStatus.HasFlag(X509ChainStatusFlags.NotTimeValid))
        {
            var notBefore = certificate.NotBefore.ToUniversalTime().ToString(TimestampFormat, CultureInfo.InvariantCulture);
            var notAfter = certificate.NotAfter.ToUniversalTime().ToString(TimestampFormat, CultureInfo.InvariantCulture);
            text += $" (valid from {notBefore} to {notAfter})";
        }

        return chainStatus.HasFlag(X509ChainStatusFlags.NotValidForUsage) ? text + UsageHint : text;
    }

    private static string FormatReason(X509Certificate2 certificate, RefusalReason reason, X509ChainStatusFlags chainStatus, string presentedNodeId) => reason switch
    {
        RefusalReason.UntrustedChain => FormatChainReason(certificate, chainStatus),
        RefusalReason.MissingNodeId => "it names no NodeId",
        RefusalReason.NotConfiguredPeer => $"it names NodeId '{presentedNodeId}', which is not a configured peer",
        RefusalReason.NotExpectedPeer => $"it names NodeId '{presentedNodeId}' instead of the expected peer",
        _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unsupported refusal reason."),
    };

    private bool ShouldLog(in RefusalKey key, DateTimeOffset now)
    {
        lock (_gate)
        {
            if (_logged.TryGetValue(key, out var loggedAt))
            {
                if (now >= loggedAt && now - loggedAt < _relogInterval)
                    return false;
            }
            else if (_logged.Count >= _capacity)
            {
                _logged.Clear();
            }

            _logged[key] = now;
            return true;
        }
    }

    private void Refuse(X509Certificate2 certificate, RefusalReason reason, X509ChainStatusFlags chainStatus, string? expectedPeerNodeId, string presentedNodeId, DateTimeOffset now)
    {
        var key = new RefusalKey(CertificateDigest.Of(certificate), reason, chainStatus, expectedPeerNodeId);
        if (!ShouldLog(in key, now))
            return;

        var text = FormatReason(certificate, reason, chainStatus, presentedNodeId);
        var subject = certificate.Subject;
        var issuer = certificate.Issuer;
        var thumbprint = certificate.GetCertHashString(HashAlgorithmName.SHA256);
        if (expectedPeerNodeId == null)
            ServerLog.InboundPeerCertificateRefused(_logger, text, subject, issuer, thumbprint, _relogInterval);
        else
            ServerLog.OutboundPeerCertificateRefused(_logger, expectedPeerNodeId, text, subject, issuer, thumbprint, _relogInterval);
    }

    /// <summary>Identifies a refusal for deduplication.</summary>
    /// <param name="Digest">The digest of the refused certificate.</param>
    /// <param name="Reason">The reason of the refusal.</param>
    /// <param name="ChainStatus">The chain status flags of a chain refusal.</param>
    /// <param name="ExpectedPeer">The NodeId of the expected peer of an outbound refusal; <see langword="null" /> for an inbound one.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct RefusalKey(CertificateDigest Digest, RefusalReason Reason, X509ChainStatusFlags ChainStatus, string? ExpectedPeer);
}
