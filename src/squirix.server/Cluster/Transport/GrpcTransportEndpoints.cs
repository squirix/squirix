using System;
using System.Net.Http;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Validates and configures gRPC transport endpoints for server-to-server transport.
/// </summary>
internal static class GrpcTransportEndpoints
{
    /// <summary>
    /// Ensures the endpoint uses HTTPS gRPC transport.
    /// </summary>
    /// <param name="url">The configured endpoint URL.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="url" /> uses plaintext HTTP.</exception>
    public static void RequireHttps(string url)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);
        if (url.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"Squirix transport requires HTTPS endpoints. Plaintext 'http://' is not supported: '{url}'.",
                nameof(url));
        }
    }

    /// <summary>
    /// Creates the default HTTP handler for HTTPS gRPC channels.
    /// </summary>
    /// <returns>A handler suitable for secure gRPC transport.</returns>
    public static HttpMessageHandler CreateChannelHandler() => new SocketsHttpHandler();
}
