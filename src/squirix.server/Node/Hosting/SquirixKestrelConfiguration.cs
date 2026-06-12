using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Centralizes Kestrel listen options and transport security for the squirix node process.
/// Invariants here affect TLS listener setup — review carefully.
/// </summary>
internal static class SquirixKestrelConfiguration
{
    /// <summary>
    /// Configures Kestrel listeners: primary HTTPS for external clients and optional cluster/internal mTLS.
    /// </summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="uri">The primary HTTPS listen URI.</param>
    /// <param name="mtlsOptions">Cluster mTLS options.</param>
    /// <param name="mtlsMaterial">Loaded cluster mTLS certificate material.</param>
    public static void ConfigureKestrel(
        WebApplicationBuilder builder,
        Uri uri,
        MtlsOptions mtlsOptions,
        MtlsCertificateMaterial mtlsMaterial)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtlsMaterial);

        var mtlsEnabled = mtlsMaterial.Enabled;
        var isLoopbackHost = SquirixExternalAccessSecurity.IsLoopbackHost(uri.Host);

        _ = builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;
            kestrel.ConfigureEndpointDefaults(static options => options.Protocols = HttpProtocols.Http1AndHttp2);

            if (isLoopbackHost)
                kestrel.ListenLocalhost(uri.Port, ConfigurePrimaryEndpoint);
            else
                kestrel.ListenAnyIP(uri.Port, ConfigurePrimaryEndpoint);

            if (!mtlsEnabled)
                return;
            if (isLoopbackHost)
                kestrel.ListenLocalhost(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, mtlsMaterial));
            else
                kestrel.ListenAnyIP(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, mtlsMaterial));
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

    /// <summary>
    /// Validates an inbound cluster mTLS client certificate against the configured cluster trust root.
    /// </summary>
    /// <param name="clientCertificate">The presented client certificate.</param>
    /// <param name="trustAnchor">Configured cluster trust root.</param>
    /// <returns><see langword="true" /> when the certificate is trusted for inter-node traffic.</returns>
    internal static bool ValidateClientCertificate(
        X509Certificate2? clientCertificate,
        X509Certificate2 trustAnchor) =>
        MtlsClientCertificateValidator.Validate(clientCertificate, trustAnchor);

    private static void ConfigurePrimaryEndpoint(ListenOptions listenOptions)
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
        _ = listenOptions.UseHttps();
    }

    private static void ConfigureMtlsEndpoint(ListenOptions listenOptions, MtlsCertificateMaterial material)
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
        _ = listenOptions.UseHttps(https => ConfigureMutualTls(https, material));
    }

    private static void ConfigureMutualTls(HttpsConnectionAdapterOptions https, MtlsCertificateMaterial material)
    {
        https.ServerCertificate = material.NodeCertificate;
        https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
        https.ClientCertificateValidation = (certificate, _, _) =>
            ValidateClientCertificate(certificate, material.TrustAnchor!);
    }
}
