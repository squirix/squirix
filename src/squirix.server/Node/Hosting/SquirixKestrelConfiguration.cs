using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Centralizes Kestrel listen options and transport security for the squirix node process.
/// Invariants here affect TLS listener setup — review carefully.
/// </summary>
internal static class SquirixKestrelConfiguration
{
    /// <summary>
    /// Configures Kestrel listeners: primary HTTPS (HTTP/1.1 and HTTP/2).
    /// </summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="uri">The primary HTTPS listen URI.</param>
    public static void ConfigureKestrel(WebApplicationBuilder builder, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(uri);

        _ = builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;
            kestrel.ConfigureEndpointDefaults(static options => options.Protocols = HttpProtocols.Http1AndHttp2);

            var isLoopbackHost = SquirixExternalAccessSecurity.IsLoopbackHost(uri.Host);
            if (isLoopbackHost)
                kestrel.ListenLocalhost(uri.Port, ConfigurePrimaryEndpoint);
            else
                kestrel.ListenAnyIP(uri.Port, ConfigurePrimaryEndpoint);

            return;

            static void ConfigurePrimaryEndpoint(ListenOptions listenOptions)
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
                _ = listenOptions.UseHttps();
            }
        });
    }

    /// <summary>
    /// Ensures the node URL uses HTTPS gRPC transport.
    /// </summary>
    /// <param name="cluster">Cluster configuration including the node URL.</param>
    /// <exception cref="InvalidOperationException">Thrown when the node URL uses plaintext HTTP.</exception>
    [SuppressMessage("ReSharper", "RedundantEmptySwitchSection", Justification = "Switch is used to throw exception")]
    public static void EnsureHttpsTransport(ClusterConfig cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        if (!Uri.TryCreate(cluster.Url, UriKind.Absolute, out var uri) ||
            !uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Squirix transport requires HTTPS. Plaintext 'http://' is not supported. Provided URL: {cluster.Url}");
        }
    }
}
