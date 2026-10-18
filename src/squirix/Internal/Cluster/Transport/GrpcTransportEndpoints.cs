using System;
using System.Net.Http;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Validates and configures gRPC transport endpoints for the client package.</summary>
internal static class GrpcTransportEndpoints
{
    /// <summary>
    /// How long a connection may receive nothing before the handler pings the node, whether or not a call is in flight, so a node whose host
    /// went silent without closing the connection is found while the connection is idle. It is the shortest delay the handler accepts.
    /// </summary>
    /// <remarks>
    /// A silent connection is closed after the delay, the ping timeout and up to two one-second handler ticks, four to five seconds, and the
    /// next call connects anew instead of waiting out its per-attempt timeout on it; a connect to a host that is still silent is bounded only
    /// by that timeout. An idle connection costs one ping and its acknowledgement per second.
    /// </remarks>
    private static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(1);

    /// <summary>How long the handler waits for the acknowledgement of a ping before it closes the connection.</summary>
    /// <remarks>Two seconds, not the one-second minimum, so a node that answers late because it paused does not lose a healthy connection.</remarks>
    private static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
    /// <returns>A handler suitable for secure gRPC transport that pings its connections with HTTP/2 keepalive.</returns>
    internal static HttpMessageHandler CreateChannelHandler() => new SocketsHttpHandler
    {
        KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
        KeepAlivePingDelay = KeepAlivePingDelay,
        KeepAlivePingTimeout = KeepAlivePingTimeout,
    };

    /// <summary>Ensures the endpoint uses HTTPS gRPC transport.</summary>
    /// <param name="uri">The configured endpoint URI.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="uri" /> uses plaintext HTTP.</exception>
    internal static void RequireHttps(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Squirix transport requires HTTPS endpoints. Plaintext 'http://' is not supported.", nameof(uri));
    }
}
