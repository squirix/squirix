using Microsoft.AspNetCore.Http;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Decides whether a request arrived on the internal mTLS listener, judged by the accepted connection and never by request headers.</summary>
internal static class InternalListenerConnection
{
    /// <summary>Checks that the connection was accepted on the internal listener port.</summary>
    /// <param name="httpContext">The request context.</param>
    /// <param name="internalListenPort">The configured internal listener port.</param>
    /// <returns><see langword="true" /> when the request arrived on <paramref name="internalListenPort" />.</returns>
    internal static bool ArrivedOnInternalPort(HttpContext httpContext, int internalListenPort)
        => internalListenPort > 0 && httpContext.Connection.LocalPort == internalListenPort;

    /// <summary>
    /// Checks that the connection was accepted on the internal listener port and presented a client certificate; the listener only completes
    /// the handshake for certificates that pass the cluster trust validation.
    /// </summary>
    /// <param name="httpContext">The request context.</param>
    /// <param name="internalListenPort">The configured internal listener port.</param>
    /// <returns><see langword="true" /> when the request came from a certificate-verified peer on the internal listener.</returns>
    internal static bool ArrivedWithPeerCertificate(HttpContext httpContext, int internalListenPort)
        => ArrivedOnInternalPort(httpContext, internalListenPort) && httpContext.Connection.ClientCertificate != null;
}
