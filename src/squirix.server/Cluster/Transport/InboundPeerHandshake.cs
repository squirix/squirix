using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Builds the TLS handshake of the internal mTLS listener: the node presents its certificate and requires a client certificate that <see cref="PeerHandshakeVerifier" /> trusts.</summary>
/// <remarks>
/// The handshake runs through a per-connection callback so the verifier can name the remote endpoint of a refused connection. The trust decision is the
/// one of <see cref="PeerHandshakeVerifier.VerifyConfiguredPeer" /> alone: the operating system chain policy errors of the TLS stack do not decide.
/// </remarks>
[Immutable]
internal sealed class InboundPeerHandshake
{
    private readonly string[] _remotePeerNodeIds;
    private readonly X509Certificate2 _serverCertificate;
    private readonly PeerHandshakeVerifier _verifier;

    private InboundPeerHandshake(PeerHandshakeVerifier verifier, X509Certificate2 serverCertificate, string[] remotePeerNodeIds)
    {
        _verifier = verifier;
        _serverCertificate = serverCertificate;
        _remotePeerNodeIds = remotePeerNodeIds;
    }

    /// <summary>Creates the handshake options of the internal mTLS listener.</summary>
    /// <param name="verifier">The verifier that decides and logs the client certificate check.</param>
    /// <param name="serverCertificate">The certificate this node presents.</param>
    /// <param name="remotePeerNodeIds">Configured remote peer node identifiers.</param>
    /// <returns>The handshake callback options for UseHttps.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal static TlsHandshakeCallbackOptions Create(PeerHandshakeVerifier verifier, X509Certificate2 serverCertificate, string[] remotePeerNodeIds)
    {
        ArgumentNullException.ThrowIfNull(verifier);
        ArgumentNullException.ThrowIfNull(serverCertificate);
        ArgumentNullException.ThrowIfNull(remotePeerNodeIds);
        var handshake = new InboundPeerHandshake(verifier, serverCertificate, remotePeerNodeIds);
        return new TlsHandshakeCallbackOptions { OnConnection = handshake.AuthenticateAsync };
    }

    private ValueTask<SslServerAuthenticationOptions> AuthenticateAsync(TlsHandshakeCallbackContext context)
    {
        var remoteEndpoint = context.Connection.RemoteEndPoint?.ToString();
        return ValueTask.FromResult(
            new SslServerAuthenticationOptions
            {
                ServerCertificate = _serverCertificate,
                ClientCertificateRequired = true,
                ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11],
                RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate != null && Verify(certificate, remoteEndpoint),
            });
    }

    private bool Verify(X509Certificate certificate, string? remoteEndpoint)
    {
        if (certificate is X509Certificate2 presented)
            return _verifier.VerifyConfiguredPeer(presented, _remotePeerNodeIds, remoteEndpoint);

        using var copy = new X509Certificate2(certificate);
        return _verifier.VerifyConfiguredPeer(copy, _remotePeerNodeIds, remoteEndpoint);
    }
}
