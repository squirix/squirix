using System;
using System.Net;
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
    private readonly SslStreamCertificateContext _serverCertificateContext;
    private readonly PeerHandshakeVerifier _verifier;

    /// <summary>Initializes a new instance of the <see cref="InboundPeerHandshake" /> class.</summary>
    /// <param name="verifier">The verifier that decides and logs the client certificate check.</param>
    /// <param name="serverCertificate">The certificate this node presents; its chain context is built once, not per handshake.</param>
    /// <param name="remotePeerNodeIds">Configured remote peer node identifiers.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal InboundPeerHandshake(PeerHandshakeVerifier verifier, X509Certificate2 serverCertificate, string[] remotePeerNodeIds)
    {
        ArgumentNullException.ThrowIfNull(verifier);
        ArgumentNullException.ThrowIfNull(serverCertificate);
        ArgumentNullException.ThrowIfNull(remotePeerNodeIds);
        _verifier = verifier;
        _serverCertificateContext = SslStreamCertificateContext.Create(serverCertificate, null, false);
        _remotePeerNodeIds = remotePeerNodeIds;
    }

    /// <summary>Creates the handshake options of the internal mTLS listener.</summary>
    /// <param name="verifier">The verifier that decides and logs the client certificate check.</param>
    /// <param name="serverCertificate">The certificate this node presents.</param>
    /// <param name="remotePeerNodeIds">Configured remote peer node identifiers.</param>
    /// <returns>The handshake callback options for UseHttps.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    internal static TlsHandshakeCallbackOptions Create(PeerHandshakeVerifier verifier, X509Certificate2 serverCertificate, string[] remotePeerNodeIds) =>
        new() { OnConnection = new InboundPeerHandshake(verifier, serverCertificate, remotePeerNodeIds).AuthenticateAsync };

    /// <summary>Builds the TLS server options of one connection.</summary>
    /// <param name="remoteEndpoint">The remote endpoint of the connection, named by a refusal log line.</param>
    /// <returns>Options that share the server certificate context of this handshake and require a client certificate.</returns>
    internal SslServerAuthenticationOptions CreateServerOptions(EndPoint? remoteEndpoint) => new()
    {
        ServerCertificateContext = _serverCertificateContext,
        ClientCertificateRequired = true,
        RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate != null && Verify(certificate, remoteEndpoint),
    };

    private ValueTask<SslServerAuthenticationOptions> AuthenticateAsync(TlsHandshakeCallbackContext context) =>
        ValueTask.FromResult(CreateServerOptions(context.Connection.RemoteEndPoint));

    private bool Verify(X509Certificate certificate, EndPoint? remoteEndpoint)
    {
        if (certificate is X509Certificate2 presented)
            return _verifier.VerifyConfiguredPeer(presented, _remotePeerNodeIds, remoteEndpoint);

        using var copy = new X509Certificate2(certificate);
        return _verifier.VerifyConfiguredPeer(copy, _remotePeerNodeIds, remoteEndpoint);
    }
}
