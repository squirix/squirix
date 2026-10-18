using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Remembers the client certificates of open connections that passed the peer check, so later calls on such a connection ask the chain cache
/// only once.
/// </summary>
/// <remarks>
/// A connection keeps one certificate instance for its lifetime, so the instance stands for the connection; the table holds it weakly and
/// drops the entry once the connection is gone. A remembered certificate is trusted only inside the validity window of the chain it was
/// verified with, so a certificate that expires while its connection is open is refused from that moment on.
/// </remarks>
[ThreadSafe]
internal sealed class VerifiedPeerCertificates
{
    private readonly PeerChainCache _chains;
    private readonly TimeProvider _clock;
    private readonly string[] _nodeIds;
    private readonly ConditionalWeakTable<X509Certificate2, Verdict> _verified = [];

    /// <summary>Initializes a new instance of the <see cref="VerifiedPeerCertificates" /> class.</summary>
    /// <param name="chains">The cache of certificate chains built against the cluster trust root.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <param name="clock">The clock that decides whether a certificate is valid; the host passes the system clock, as the TLS handshake uses real time.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal VerifiedPeerCertificates(PeerChainCache chains, string[] nodeIds, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(chains);
        ArgumentNullException.ThrowIfNull(nodeIds);
        ArgumentNullException.ThrowIfNull(clock);
        _chains = chains;
        _nodeIds = nodeIds;
        _clock = clock;
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
        if (!_verified.TryGetValue(certificate, out var verdict))
        {
            if (!_chains.TryVerify(certificate, now, out var chainVerdict) || !MtlsTopology.IsRemotePeer(_nodeIds, chainVerdict.NodeId))
            {
                nodeId = string.Empty;
                return false;
            }

            verdict = new Verdict(chainVerdict);
            _verified.AddOrUpdate(certificate, verdict);
        }

        var valid = verdict.Chain.IsValidAt(now);
        nodeId = valid ? verdict.Chain.NodeId : string.Empty;
        return valid;
    }

    /// <summary>The chain verdict of a verified connection certificate, boxed so the weak table can hold it.</summary>
    /// <param name="Chain">The NodeId and validity window of the certificate chain.</param>
    [Immutable]
    private sealed record Verdict(PeerChainVerdict Chain);
}
