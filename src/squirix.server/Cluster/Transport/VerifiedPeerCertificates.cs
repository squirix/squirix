using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Remembers the client certificates of open connections that passed the peer check, so later calls on such a connection ask the chain cache
/// only once.
/// </summary>
/// <remarks>
/// A connection keeps one certificate instance for its lifetime, so the instance stands for the connection; the table holds it weakly and
/// drops the entry once the connection is gone. A remembered certificate is trusted only inside the validity window of the chain it was
/// verified with, so a certificate that expires while its connection is open is refused from that moment on. That is logged once per connection
/// certificate, not once per refused call.
/// </remarks>
[ThreadSafe]
internal sealed class VerifiedPeerCertificates
{
    private readonly PeerChainCache _chains;
    private readonly TimeProvider _clock;
    private readonly ILogger _logger;
    private readonly string[] _nodeIds;
    private readonly ConditionalWeakTable<X509Certificate2, Verdict> _verdicts = [];

    /// <summary>Initializes a new instance of the <see cref="VerifiedPeerCertificates" /> class.</summary>
    /// <param name="chains">The cache of certificate chains built against the cluster trust root.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid; the host passes the system clock, as the TLS handshake uses real time.</param>
    /// <param name="logger">The logger a certificate that expired on an open connection is reported to.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal VerifiedPeerCertificates(PeerChainCache chains, string[] nodeIds, TimeProvider clock, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(chains);
        ArgumentNullException.ThrowIfNull(nodeIds);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(logger);
        _chains = chains;
        _nodeIds = nodeIds;
        _clock = clock;
        _logger = logger;
    }

    /// <summary>Creates the verifier for a node, or none when internode mTLS is disabled.</summary>
    /// <param name="cluster">The cluster topology that names the remote peers.</param>
    /// <param name="mtls">The mTLS material of the node.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid.</param>
    /// <param name="logger">The logger a certificate that expired on an open connection is reported to.</param>
    /// <returns>The verifier, or <see langword="null" /> when <paramref name="mtls" /> is not enabled.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal static VerifiedPeerCertificates? CreateIfEnabled(TopologyOptions cluster, MtlsCertificate mtls, TimeProvider clock, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtls);
        return mtls is { Enabled: true, PeerChains: { } peerChains } ? new VerifiedPeerCertificates(peerChains, MtlsTopology.GetRemotePeerNodeIds(cluster), clock, logger) : null;
    }

    /// <summary>Checks a peer certificate against the cluster trust root and the configured peers, consulting the chain cache only the first time.</summary>
    /// <param name="certificate">The client certificate of the connection.</param>
    /// <param name="nodeId">The NodeId of the certificate when it is trusted.</param>
    /// <returns><see langword="true" /> when the certificate chains to the trust root, is valid now and names a configured peer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="certificate" /> is <see langword="null" />.</exception>
    internal bool TryVerify(X509Certificate2 certificate, out string nodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        var now = _clock.GetUtcNow();
        if (!_verdicts.TryGetValue(certificate, out var verdict))
        {
            if (!_chains.TryVerify(certificate, now, out var chainVerdict) || !MtlsTopology.IsRemotePeer(_nodeIds, chainVerdict.NodeId))
            {
                nodeId = string.Empty;
                return false;
            }

            verdict = new Verdict(in chainVerdict);
            _verdicts.AddOrUpdate(certificate, verdict);
        }

        var valid = verdict.Chain.IsValidAt(now);
        nodeId = valid ? verdict.Chain.NodeId : string.Empty;
        if (!valid && now > verdict.Chain.NotAfter && verdict.TryMarkExpiredLogged())
            ServerLog.PeerCertificateExpiredOnOpenConnection(_logger, verdict.Chain.NodeId, verdict.Chain.NotAfter);

        return valid;
    }

    /// <summary>The chain verdict of a verified connection certificate, boxed so the weak table can hold it, and whether its expiry was logged.</summary>
    [ThreadSafe]
    private sealed class Verdict
    {
        private int _expiredLogged;

        /// <summary>Initializes a new instance of the <see cref="Verdict" /> class.</summary>
        /// <param name="chain">The NodeId and validity window of the certificate chain.</param>
        internal Verdict(in PeerChainVerdict chain)
        {
            Chain = chain;
        }

        /// <summary>Gets the NodeId and validity window of the certificate chain.</summary>
        internal PeerChainVerdict Chain { get; }

        /// <summary>Marks the expiry of the certificate as logged.</summary>
        /// <returns><see langword="true" /> for the first caller only.</returns>
        internal bool TryMarkExpiredLogged() => Interlocked.Exchange(ref _expiredLogged, 1) == 0;
    }
}
