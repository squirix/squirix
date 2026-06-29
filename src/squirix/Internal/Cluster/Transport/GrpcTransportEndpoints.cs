using System;
using System.Net.Http;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Validates and configures gRPC transport endpoints for the client package.</summary>
internal static class GrpcTransportEndpoints
{
    /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static HttpMessageHandler CreateChannelHandler() => new SocketsHttpHandler();

    /// <summary>Ensures the endpoint uses HTTPS gRPC transport.</summary>
    /// <param name="uri">The configured endpoint URL.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="uri" /> uses plaintext HTTP.</exception>
    public static void RequireHttps(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Squirix transport requires HTTPS endpoints. Plaintext 'http://' is not supported: '{uri}'.", nameof(uri));
        }
    }
}
