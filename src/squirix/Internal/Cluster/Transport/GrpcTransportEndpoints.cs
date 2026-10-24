using System;
using System.Net.Http;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Validates and configures gRPC transport endpoints for the client package.</summary>
internal static class GrpcTransportEndpoints
{
    /// <summary>
    /// How long a connection may receive nothing before the handler pings the node, whether or not a call is in flight
    /// (<see cref="HttpKeepAlivePingPolicy.Always" />), so a node whose host went silent without closing the connection is found while the
    /// connection is idle, before the next call is written to it.
    /// </summary>
    /// <remarks>
    /// Far longer than the one second the server-to-server channels use: a client talks to the node over arbitrary networks, and an idle
    /// connection costs one ping and its acknowledgement per delay. A silent connection is closed after this delay plus
    /// <see cref="KeepAlivePingTimeout" /> plus up to two one-second handler ticks, so a call written to it meanwhile still ends at its deadline.
    /// </remarks>
    internal static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(10);

    /// <summary>How long the handler waits for the acknowledgement of a ping before it closes the connection.</summary>
    /// <remarks>
    /// Longer than the per-attempt connect timeout, so a ping that a loaded node or this process answers late (a garbage collection, a starved
    /// thread pool, a TLS reconnect storm) does not close a healthy connection and force reconnects that miss their own attempt timeout.
    /// </remarks>
    internal static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(20);

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
