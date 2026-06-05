using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Centralizes Kestrel listen options and transport security for the squirix node process.
/// Invariants here affect TLS, optional mutual TLS, and optional plaintext HTTP/1 sidecars — review carefully.
/// </summary>
internal static class SquirixKestrelConfiguration
{
    /// <summary>
    /// Configures Kestrel listeners: primary HTTP/2 (TLS or cleartext per <paramref name="isHttps" />),
    /// optional mTLS when <c>SQUIRIX_MTLS</c> is set, and optional plaintext HTTP/1 sidecar when <c>SQUIRIX_HTTP1_PORT</c> is set.
    /// </summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="uri">The primary listen URI (scheme selects TLS vs cleartext).</param>
    /// <param name="isHttps">Whether the primary listener uses TLS.</param>
    public static void ConfigureKestrel(WebApplicationBuilder builder, Uri uri, bool isHttps)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(uri);

        _ = builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.AddServerHeader = false;
            if (!isHttps)
                kestrel.AllowAlternateSchemes = true;

            // Precompute flags once
            var mtlsEnabled = EnvVariables.ReadBool("SQUIRIX_MTLS");
            var allowSelfSigned = EnvVariables.ReadBool("SQUIRIX_MTLS_ALLOW_SELF_SIGNED");

            if (!isHttps)
                kestrel.ConfigureEndpointDefaults(static options => options.Protocols = HttpProtocols.Http2);

            var isLoopbackHost = SquirixExternalAccessSecurity.IsLoopbackHost(uri.Host);
            if (isLoopbackHost)
            {
                kestrel.ListenLocalhost(uri.Port, ConfigurePrimaryEndpoint);
                if (!isHttps)
                    TryAddHttp1SidecarListener(kestrel, false, builder.Environment.EnvironmentName);
            }
            else
            {
                kestrel.ListenAnyIP(uri.Port, ConfigurePrimaryEndpoint);
                if (!isHttps)
                    TryAddHttp1SidecarListener(kestrel, true, builder.Environment.EnvironmentName);
            }

            return;

            void ConfigurePrimaryEndpoint(ListenOptions listenOptions)
            {
                listenOptions.Protocols = HttpProtocols.Http2;

                if (!isHttps)
                    return;

                if (!mtlsEnabled)
                {
                    _ = listenOptions.UseHttps();
                    return;
                }

                _ = listenOptions.UseHttps(https => ConfigureMutualTls(https, allowSelfSigned));
            }
        });
        return;

        static void ConfigureMutualTls(HttpsConnectionAdapterOptions https, bool allowSelfSigned)
        {
            https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
            https.ClientCertificateValidation = (cert, chain, _) =>
            {
                try
                {
                    if (chain is null)
                        return allowSelfSigned;

                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

                    var ok = chain.Build(cert);
                    return ok || allowSelfSigned;
                }
                catch
                {
                    return false;
                }
            };
        }
    }

    /// <summary>
    /// Enforces HTTPS-by-default outside Development and enables HTTP/2 cleartext when plaintext is explicitly allowed.
    /// </summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="cluster">Cluster configuration including the node URL.</param>
    /// <param name="isHttps">Whether the primary listener uses TLS.</param>
    /// <param name="allowHttpInAnyEnvironment">When <see langword="true" />, allows cleartext outside Development.</param>
    [SuppressMessage("ReSharper", "RedundantEmptySwitchSection", Justification = "Switch is used to throw exception")]
    public static void EnsureHttpMode(WebApplicationBuilder builder, ClusterConfig cluster, bool isHttps, bool allowHttpInAnyEnvironment)
    {
        var isDev = string.Equals(builder.Environment.EnvironmentName, "Development", StringComparison.OrdinalIgnoreCase);
        switch (isHttps)
        {
            case false when !(isDev || allowHttpInAnyEnvironment):
                throw new InvalidOperationException($"Squirix node must run over HTTPS in {builder.Environment.EnvironmentName}. Provided URL: {cluster.Url}");

            case false:
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                break;

            default:
                break;
        }
    }

    private static void TryAddHttp1SidecarListener(KestrelServerOptions k, bool listenAnyIp, string environmentName)
    {
        var http1PortOrNull = EnvVariables.ReadInt("SQUIRIX_HTTP1_PORT");
        if (http1PortOrNull is null)
            return;

        var http1Port = http1PortOrNull.Value;
        if (http1Port <= 0)
            throw new InvalidOperationException($"Invalid SQUIRIX_HTTP1_PORT value '{http1Port}'. Expected a positive TCP port.");

        if (listenAnyIp)
        {
            if (!EnvVariables.ReadBool("SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL"))
            {
                throw new InvalidOperationException(
                    $"Refusing plaintext HTTP sidecar on non-loopback interface (SQUIRIX_HTTP1_PORT={http1Port}). " +
                    "Set SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true only for explicitly insecure scenarios.");
            }

            if (!string.Equals(environmentName, "Development", StringComparison.OrdinalIgnoreCase))
                Console.Error.WriteLine($"WARNING: plaintext HTTP sidecar is exposed on non-loopback interface (SQUIRIX_HTTP1_PORT={http1Port}, environment={environmentName}).");
        }

        if (listenAnyIp)
            k.ListenAnyIP(http1Port, static lo => { lo.Protocols = HttpProtocols.Http1; });
        else
            k.ListenLocalhost(http1Port, static lo => { lo.Protocols = HttpProtocols.Http1; });
    }
}
