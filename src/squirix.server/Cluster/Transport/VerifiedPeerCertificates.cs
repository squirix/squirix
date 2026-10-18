using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Remembers the client certificates of open connections that passed the peer check, so later calls on such a connection build no
/// certificate chain.
/// </summary>
/// <remarks>
/// A connection keeps one certificate instance for its lifetime, so the instance stands for the connection; the table holds it weakly and
/// drops the entry once the connection is gone. A remembered certificate is trusted only inside the validity window of the chain it was
/// verified with, so a certificate that expires while its connection is open is refused from that moment on.
/// </remarks>
internal sealed class VerifiedPeerCertificates
{
    private readonly TimeProvider _clock;
    private readonly string[] _nodeIds;
    private readonly X509Certificate2 _trustAnchor;
    private readonly ConditionalWeakTable<X509Certificate2, Verdict> _verified = [];
    private long _chainBuilds;

    /// <summary>Initializes a new instance of the <see cref="VerifiedPeerCertificates" /> class.</summary>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <param name="nodeIds">Configured remote peer node identifiers.</param>
    /// <param name="clock">The clock that decides whether a certificate is still valid.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal VerifiedPeerCertificates(X509Certificate2 trustAnchor, string[] nodeIds, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(trustAnchor);
        ArgumentNullException.ThrowIfNull(nodeIds);
        ArgumentNullException.ThrowIfNull(clock);
        _trustAnchor = trustAnchor;
        _nodeIds = nodeIds;
        _clock = clock;
    }

    /// <summary>Gets the number of certificate chains built so far.</summary>
    internal long ChainBuilds => Interlocked.Read(ref _chainBuilds);

    /// <summary>Checks a peer certificate against the cluster trust root and the configured peers, building its chain only the first time.</summary>
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
            _ = Interlocked.Increment(ref _chainBuilds);
            if (!MtlsClientCertificateValidator.TryValidateForConfiguredRemotePeer(certificate, _trustAnchor, _nodeIds, now, out var window, out var verifiedNodeId))
            {
                nodeId = string.Empty;
                return false;
            }

            verdict = new Verdict(verifiedNodeId, window.NotBefore, window.NotAfter);
            _verified.AddOrUpdate(certificate, verdict);
        }

        nodeId = verdict.NodeId;
        return now >= verdict.NotBefore && now <= verdict.NotAfter;
    }

    /// <summary>The outcome of a chain build: the NodeId of the certificate and the window in which every certificate of its chain is valid.</summary>
    /// <param name="NodeId">The NodeId of the verified certificate.</param>
    /// <param name="NotBefore">The latest start of validity among the certificates of the chain.</param>
    /// <param name="NotAfter">The earliest end of validity among the certificates of the chain.</param>
    private sealed record Verdict(string NodeId, DateTimeOffset NotBefore, DateTimeOffset NotAfter);
}
