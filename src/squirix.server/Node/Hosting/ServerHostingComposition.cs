using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Hosting;

internal static class ServerHostingComposition
{
    [SuppressMessage(
        "Microsoft.Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Cluster mTLS material is registered as a singleton and disposed by the host on shutdown.")]
    internal static async Task ConfigureBuilderAsync(WebApplicationBuilder builder, ClusterConfig cluster, CompositionArgs args, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(args);

        var persistence = args.PersistenceOptions is null ? null : PersistenceOptionsResolver.Resolve(cluster, args.PersistenceOptions);
        var persistenceEnabled = persistence is not null;
        var uri = cluster.Uri;
        _ = builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        KestrelConfiguration.EnsureHttpsTransport(cluster);
        var requiresInterNodeMtls = MtlsTopology.RequiresInterNodeMtls(cluster);
        var mtlsOptions = args.MtlsOptions ?? MtlsOptionsResolver.ResolveFromEnvironment();
        var mtlsMaterial = args.MtlsMaterial ?? MtlsCertificateMaterial.Load(mtlsOptions, uri.Port, requiresInterNodeMtls, cluster.NodeId);
        KestrelConfiguration.ConfigureKestrel(builder, uri, cluster, mtlsOptions, mtlsMaterial);

        _ = await builder.Services.AddSquirixValidatedOptionsAsync(
            cluster,
            new ValidatedOptionsArgs
            {
                SnapshotOptions = args.SnapshotOptions,
                BackpressureOptions = args.BackpressureOptions,
                PersistenceOptions = persistence,
                MemoryPressureOptions = args.MemoryPressureOptions,
                MtlsOptions = mtlsOptions,
                MtlsMaterial = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);
        _ = builder.Services.AddSquirixRuntimeServices();
        _ = builder.Services.AddSquirixClusterServices(cluster, args.CallPolicyFactory, args.PeerHandlerFactory);
        if (persistenceEnabled)
            _ = await builder.Services.AddSquirixPersistenceServicesAsync(persistence!, args.WaitForRecovery, cancellationToken).ConfigureAwait(false);

        _ = builder.Services.AddSquirixCachePipeline(args.Extensions, persistenceEnabled);
        _ = builder.Services.AddSquirixNodeEndpointServices(persistenceEnabled);
        var authEnabled = builder.Services.AddSquirixSecurityServices(args.SecurityOptions);
        ExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(uri, authEnabled);
        _ = builder.Services.AddSquirixFrameworkServices(builder.Environment.IsDevelopment(), args.ConfigureGrpc);
        _ = builder.Services.AddSquirixGrpcCorrelationInterceptor();
        args.ServicesConfigure?.Invoke(builder.Services);
        args.Extensions?.ConfigureServices?.Invoke(builder.Services);
        if (args.Extensions is not null)
            _ = builder.Services.AddSingleton(args.Extensions);
        _ = builder.Services.AddSingleton(new SquirixServerEndpointMappingOptions(authEnabled));
    }

    internal static WebApplication MapServer(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        _ = app.Use(static async (context, next) =>
        {
            try
            {
                await next().ConfigureAwait(false);
            }
            catch (ResourceExhaustedException ex)
            {
                await ex.ToHttpResult().ExecuteAsync(context).ConfigureAwait(false);
            }
            catch (SquirixException ex)
            {
                await ex.ToHttpResult().ExecuteAsync(context).ConfigureAwait(false);
            }
        });

        var options = app.Services.GetRequiredService<SquirixServerEndpointMappingOptions>();
        if (!options.AuthEnabled)
            return MapEndpoints(app, options.AuthEnabled);
        _ = app.UseAuthentication();
        _ = app.UseAuthorization();

        return MapEndpoints(app, options.AuthEnabled);
    }

    internal static Task ConfigureBuilderAsync(
        WebApplicationBuilder builder,
        SquirixServerOptions options,
        SquirixServerExtensionOptions? extensions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        var cluster = Configurator.ToClusterConfig(options);
        return ConfigureBuilderAsync(
            builder,
            cluster,
            new CompositionArgs
            {
                WaitForRecovery = options.WaitForRecovery,
                PersistenceOptions = ResolvePersistenceOptions(options),
                Extensions = extensions,
            },
            cancellationToken);
    }

    private static WebApplication MapEndpoints(WebApplication app, bool authEnabled)
    {
        _ = app.MapSquirixEndpoints(authEnabled);
        var extensions = app.Services.GetService<SquirixServerExtensionOptions>();
        extensions?.MapEndpoints?.Invoke(app);
        extensions?.MapEndpointsWithAuthorization?.Invoke(app, authEnabled);
        return app;
    }

    private static PersistenceOptions? ResolvePersistenceOptions(SquirixServerOptions options)
    {
        if (!options.PersistenceEnabled)
            return null;

        var resolvePersistenceOptions = new PersistenceOptions
        {
            JournalMaxSegmentMb = 64,
            FlushIntervalMs = 10,
        };
        return string.IsNullOrWhiteSpace(options.DataDirectory) ? resolvePersistenceOptions : new PersistenceOptions { DataDir = options.DataDirectory };
    }

    private sealed record SquirixServerEndpointMappingOptions(bool AuthEnabled);

    /// <summary>
    /// Centralizes Kestrel listen options and transport security for the squirix node process.
    /// Invariants here affect TLS listener setup — review carefully.
    /// </summary>
    private static class KestrelConfiguration
    {
        /// <summary>Configures Kestrel listeners: primary HTTPS for external clients and optional cluster/internal mTLS.</summary>
        /// <param name="builder">The web application builder.</param>
        /// <param name="uri">The primary HTTPS listen URI.</param>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="mtlsOptions">Cluster mTLS options.</param>
        /// <param name="mtlsMaterial">Loaded cluster mTLS certificate material.</param>
        internal static void ConfigureKestrel(WebApplicationBuilder builder, Uri uri, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(uri);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(mtlsOptions);
            ArgumentNullException.ThrowIfNull(mtlsMaterial);

            var mtlsEnabled = mtlsMaterial.Enabled;
            var remotePeerNodeIds = MtlsTopology.GetRemotePeerNodeIds(cluster);
            var isLoopbackHost = ExternalAccessSecurity.IsLoopbackHost(uri.Host);

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
                    kestrel.ListenLocalhost(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, mtlsMaterial, remotePeerNodeIds));
                else
                    kestrel.ListenAnyIP(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, mtlsMaterial, remotePeerNodeIds));
            });
        }

        /// <summary>Ensures the node URL uses HTTPS gRPC transport.</summary>
        /// <param name="cluster">Cluster configuration including the node URL.</param>
        /// <exception cref="InvalidOperationException">Thrown when the node URL uses plaintext HTTP.</exception>
        internal static void EnsureHttpsTransport(ClusterConfig cluster)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            if (!cluster.Uri.IsAbsoluteUri || !cluster.Uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Squirix transport requires HTTPS. Plaintext 'http://' is not supported. Provided URL: {cluster.Uri}");
            }
        }

        private static void ConfigureMtlsEndpoint(ListenOptions listenOptions, MtlsCertificateMaterial material, string[] nodeIds)
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
            _ = listenOptions.UseHttps(https => ConfigureMutualTls(https, material, nodeIds));
        }

        private static void ConfigureMutualTls(HttpsConnectionAdapterOptions https, MtlsCertificateMaterial material, string[] remotePeerNodeIds)
        {
            https.ServerCertificate = material.NodeCertificate;
            https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
            https.ClientCertificateValidation = (certificate, _, _) =>
                MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, material.TrustAnchor!, remotePeerNodeIds);
        }

        private static void ConfigurePrimaryEndpoint(ListenOptions listenOptions)
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
            _ = listenOptions.UseHttps();
        }
    }
}
