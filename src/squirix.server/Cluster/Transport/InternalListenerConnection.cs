using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Http;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Decides whether a request arrived on the internal mTLS listener, judged by the accepted connection and never by request headers.</summary>
internal static class InternalListenerConnection
{
    /// <summary>Marks a connection as accepted by the internal listener; only connection middleware of that listener calls this.</summary>
    /// <param name="connection">The accepted connection.</param>
    internal static void Mark(ConnectionContext connection)
        => connection.Features.Set(InternalListenerMarker.Instance);

    /// <summary>Checks that the connection was accepted by the internal listener, on its port.</summary>
    /// <param name="httpContext">The request context.</param>
    /// <param name="internalListenPort">The configured internal listener port.</param>
    /// <returns><see langword="true" /> when the connection carries the internal listener marker and its local port is <paramref name="internalListenPort" />.</returns>
    internal static bool ArrivedOnInternalPort(HttpContext httpContext, int internalListenPort)
        => internalListenPort > 0 && httpContext.Connection.LocalPort == internalListenPort && httpContext.Features.Get<InternalListenerMarker>() != null;

    /// <summary>
    /// Checks that the connection was accepted by the internal listener and presented a client certificate; the listener only completes
    /// the handshake for certificates that pass the cluster trust validation.
    /// </summary>
    /// <param name="httpContext">The request context.</param>
    /// <param name="internalListenPort">The configured internal listener port.</param>
    /// <returns><see langword="true" /> when the request came from a certificate-bearing peer on the internal listener.</returns>
    internal static bool ArrivedWithPeerCertificate(HttpContext httpContext, int internalListenPort)
        => ArrivedOnInternalPort(httpContext, internalListenPort) && httpContext.Connection.ClientCertificate != null;
}
