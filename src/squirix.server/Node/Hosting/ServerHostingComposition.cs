using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class ServerHostingComposition
{
    /// <summary>Maximum queued follower repairs, excluding the active repair.</summary>
    private const int RepairQueueCapacity = 128;

    /// <summary>Configures the node web host builder from cluster topology and optional composition overrides.</summary>
    /// <param name="builder">The web application builder.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="configure">Optional composition overrides callback.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when builder configuration finishes.</returns>
    internal static Task ConfigureBuilderAsync(
        WebApplicationBuilder builder,
        TopologyOptions cluster,
        Action<ICompositionArgs>? configure = null,
        CancellationToken cancellationToken = default)
    {
        var args = new CompositionArgs();
        configure?.Invoke(args);
        return ConfigureBuilderCoreAsync(builder, cluster, args, cancellationToken);
    }

    internal static WebApplication MapServer(WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        LogManager.Configure(app.Services.GetRequiredService<ILoggerFactory>());

        // The replica group registry is opened eagerly during composition but resolved lazily. Touch it
        // here so the container tracks the instance and disposes follower-log durability workers on host
        // shutdown even when no cache operation ever resolved it first; otherwise group files stay locked
        // and a restart on the same directory fails to open them.
        _ = app.Services.GetService<ReplicaGroupRegistry>();

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
            catch (JournalCapacityExceededException ex)
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

    /// <summary>Opens the replica group logs for an activated node and registers replication services.</summary>
    /// <param name="services">DI service collection.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="persistence">Resolved persistence options.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every group log is ready.</returns>
    /// <remarks>
    /// One group per peer: every group whose replica set can include this node is served locally.
    /// Logs open eagerly, so any replication RPC fails closed until its log is ready.
    /// </remarks>
    private static async Task AddReplicaGroupRegistryAsync(
        IServiceCollection services,
        TopologyOptions cluster,
        PersistenceOptions persistence,
        MtlsOptions mtlsOptions,
        CancellationToken cancellationToken)
    {
        ImmutableArray<byte> fingerprint = [.. TopologyFingerprint.CreateFromTopology(cluster, mtlsOptions).Bytes];
        await EnsureActivatedTopologyAsync(persistence.DataDir, fingerprint.AsMemory(), cluster.ConfigurationGeneration, cluster.ReplicaCount, cancellationToken)
           .ConfigureAwait(false);

        var groupIds = new string[cluster.Peers.Length];
        for (var i = 0; i < groupIds.Length; i++)
            groupIds[i] = cluster.Peers[i].NodeId;

        var registry = new ReplicaGroupRegistry(persistence.DataDir, groupIds, cluster.ReplicaCount, fingerprint.AsMemory(), cluster.ConfigurationGeneration);
        try
        {
            await registry.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await registry.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        // Factory overloads transfer disposal ownership to the container: the registry closes
        // follower-log durability workers and the committer drains its coordinator on host shutdown.
        // AddSingleton(instance) would leak both past shutdown and keep group files locked.
        _ = services.AddSingleton(_ => registry);
        _ = services.AddSingleton(sp => new ReplicaGroupCommitter(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<IReplicaGroupLocator>(),
            sp.GetRequiredService<IReplicaRpcGateway>(),
            sp.GetRequiredService<OwnershipGuardCacheDecorator<object?>>(),
            sp.GetRequiredService<TopologyOptions>().NodeId,
            fingerprint.AsMemory(),
            sp.GetRequiredService<TopologyOptions>().ConfigurationGeneration));
    }

    /// <summary>
    /// Registers cluster locator, inter-node transport, and replication planning services.
    /// Composition root for Cluster child namespaces (parent Cluster must not reference them).
    /// </summary>
    /// <remarks>
    /// Repair and bootstrap planning stay registered on every node, including RF=1 and foundation-only
    /// hosts: activation discovers them when a stopped cluster is seeded for RF&gt;1, and the idle repair
    /// service parks on its queue read without burning a thread.
    /// </remarks>
    /// <param name="services">DI service collection.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="args">Hosting composition overrides including optional peer handler factory.</param>
    private static void AddSquirixClusterStack(IServiceCollection services, TopologyOptions cluster, ICompositionArgs args)
    {
        _ = services.AddSquirixClusterLocator(cluster);
        _ = services.AddSquirixClusterTransport(cluster, null, args.PeerHandlerFactory);
        _ = services.AddSquirixClusterReplication(cluster, args.FoundationOnly);
        _ = services.AddSingleton(static _ => new ReplicaRepairService(RepairQueueCapacity));
        _ = services.AddHostedService(static sp => sp.GetRequiredService<ReplicaRepairService>());
        _ = services.AddSingleton(static _ => new BootstrapPlanner());
        if (!args.FoundationOnly && cluster.ReplicaCount <= 1)
            return;
        _ = services.AddSingleton(static sp => new SquirixReplicationServiceAdapter(
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<MtlsCertificateMaterial>(),
            sp.GetService<ReplicaGroupRegistry>()));
        _ = services.AddSingleton<IReplicaRpcGateway>(static sp => new ReplicaRpcGateway(sp.GetRequiredService<IServerClientPool>()));
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The Meter instance is transferred to the DI container via the factory overload, which disposes it on host shutdown.")]
    private static async Task ConfigureBuilderCoreAsync(WebApplicationBuilder builder, TopologyOptions cluster, ICompositionArgs args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(args);

        var persistence = args.PersistenceOptions == null ? null : PersistenceOptionsResolver.Resolve(cluster, args.PersistenceOptions);
        var persistenceEnabled = persistence != null;
        var uri = cluster.Uri;
        var (mtlsOptions, mtlsMaterial) = ResolveClusterTransportSecurity(builder, cluster, args, persistenceEnabled);

        _ = await builder.Services.AddSquirixValidatedOptionsAsync(
            cluster,
            new ValidatedOptionsArgs
            {
                BackpressureOptions = args.BackpressureOptions,
                PersistenceOptions = persistence,
                MemoryPressureOptions = args.MemoryPressureOptions,
                MtlsOptions = mtlsOptions,
                MtlsMaterial = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);

        // Per-host Meter. A single instance is created here, then both the runtime and the persistence composition
        // resolve this same singleton. It is registered through the factory overload, so the DI container takes
        // ownership and disposes of it when the owning host shuts down: AddSingleton(instance) does not transfer
        // disposal ownership in Microsoft DI, which would leak the meter.
        var serverMeter = new Meter("Squirix");
        _ = builder.Services.AddSingleton(_ => serverMeter);

        _ = builder.Services.AddSquirixRuntimeServices();
        AddSquirixClusterStack(builder.Services, cluster, args);
        if (persistenceEnabled)
        {
            _ = await builder.Services.AddPersistenceServicesAsync(persistence!, serverMeter, args.WaitForRecovery, cancellationToken).ConfigureAwait(false);

            // Follower-group storage composition. For RF=1 the local composition is empty, so no group storage is
            // materialized; group membership is derived in a later milestone. Registered only when persistence is
            // enabled because the factory resolves PersistenceOptions, which are not registered otherwise.
            // Note: GroupRecovery.RecoverAllAsync is intentionally NOT invoked from any production path in this
            // milestone; with an empty static composition a call would be a no-op. Recovery wiring is introduced
            // together with group-membership derivation (see the durable ordered follower log specification, M8-05).
            _ = builder.Services.AddSingleton(static sp => new GroupRecovery(sp.GetRequiredService<PersistenceOptions>().DataDir, GroupComposition.Empty()));
        }

        if (cluster.ReplicaCount > 1 && persistenceEnabled && !args.FoundationOnly)
            await AddReplicaGroupRegistryAsync(builder.Services, cluster, persistence!, mtlsOptions, cancellationToken).ConfigureAwait(false);

        _ = builder.Services.AddSquirixCachePipeline(args.Extensions, persistenceEnabled);
        _ = builder.Services.AddSquirixNodeEndpointServices(persistenceEnabled);
        var authEnabled = builder.Services.AddSquirixSecurityServices(args.SecurityOptions);
        ExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(uri, authEnabled);
        _ = builder.Services.AddSquirixFrameworkServices(builder.Environment.IsDevelopment(), args.ConfigureGrpc);
        _ = builder.Services.AddSquirixGrpcCorrelationInterceptor();
        args.ServicesConfigure?.Invoke(builder.Services);
        args.Extensions?.ConfigureServices?.Invoke(builder.Services);
        if (args.Extensions != null)
            _ = builder.Services.AddSingleton(args.Extensions);
        _ = builder.Services.AddSingleton(new SquirixServerEndpointMappingOptions(authEnabled));
    }

    /// <summary>Freezes the activated topology on first start and refuses later identity changes.</summary>
    /// <param name="dataDir">Exclusive node data directory.</param>
    /// <param name="fingerprint">Configured static topology fingerprint.</param>
    /// <param name="generation">Configured configuration generation.</param>
    /// <param name="replicaCount">Configured replica factor.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the configured identity is authorized.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the configured identity differs from the stamped one.</exception>
    /// <remarks>
    /// Only an offline bootstrap rewrites the stamp, so live and stopped topology changes outside
    /// the authorized migration path fail startup instead of splitting the replica set.
    /// </remarks>
    private static async Task EnsureActivatedTopologyAsync(
        string dataDir,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        int replicaCount,
        CancellationToken cancellationToken)
    {
        var store = new ActivatedTopologyStampStore(dataDir);
        var current = new ActivatedTopologyStamp { Generation = generation, Fingerprint = fingerprint, ReplicaCount = replicaCount };
        var stamped = await store.ReadAsync(cancellationToken).ConfigureAwait(false);
        if (stamped == null)
        {
            await store.PublishAsync(current, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!stamped.Matches(current))
        {
            throw new InvalidOperationException(
                $"Activated topology identity changed without an offline bootstrap (RF=1 to RF>1): stamped generation {stamped.Generation}, replica count {stamped.ReplicaCount}; configured generation {generation}, replica count {replicaCount}.");
        }
    }

    private static WebApplication MapEndpoints(WebApplication app, bool authEnabled)
    {
        _ = app.MapSquirixEndpoints(authEnabled);
        var extensions = app.Services.GetService<ExtensionOptions>();
        extensions?.MapEndpoints?.Invoke(app);
        extensions?.MapEndpointsWithAuthorization?.Invoke(app, authEnabled);
        return app;
    }

    [SuppressMessage(
        "Microsoft.Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Cluster mTLS material is registered as a singleton and disposed by the host on shutdown.")]
    private static (MtlsOptions Options, MtlsCertificateMaterial Material) ResolveClusterTransportSecurity(
        WebApplicationBuilder builder,
        TopologyOptions cluster,
        ICompositionArgs args,
        bool persistenceEnabled)
    {
        var uri = cluster.Uri;
        _ = builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        KestrelConfiguration.EnsureHttpsTransport(cluster);
        var requiresInterNodeMtls = MtlsTopology.RequiresInterNodeMtls(cluster);
        var mtlsOptions = args.MtlsOptions ?? MtlsOptionsResolver.ResolveFromEnvironment();
        ReplicationActivationGuard.ThrowIfDisallowed(cluster.ReplicaCount, persistenceEnabled, mtlsOptions);
        var mtlsMaterial = args.MtlsMaterial ?? MtlsCertificateMaterial.Load(mtlsOptions, uri.Port, requiresInterNodeMtls, cluster.NodeId);
        KestrelConfiguration.ConfigureKestrel(builder, uri, cluster, mtlsOptions, mtlsMaterial);
        return (mtlsOptions, mtlsMaterial);
    }

    [Immutable]
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
        internal static void ConfigureKestrel(WebApplicationBuilder builder, Uri uri, TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
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

        /// <summary>Ensures the node URI uses HTTPS gRPC transport.</summary>
        /// <param name="cluster">Cluster configuration including the node URI.</param>
        /// <exception cref="InvalidOperationException">Thrown when the node URI uses plaintext HTTP.</exception>
        internal static void EnsureHttpsTransport(TopologyOptions cluster)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            if (!cluster.Uri.IsAbsoluteUri || !cluster.Uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Squirix transport requires HTTPS. Plaintext 'http://' is not supported.");
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

    private static class PersistenceOptionsResolver
    {
        internal static PersistenceOptions Resolve(TopologyOptions cluster, PersistenceOptions source)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(source);

            var dataDir = string.IsNullOrWhiteSpace(source.DataDir) ? GetDefaultDataDir(cluster.ClusterId, cluster.NodeId) : source.DataDir;
            return source with { DataDir = dataDir };
        }

        private static string GetDefaultDataDir(string clusterId, string nodeId)
        {
            var testRoot = EnvVariables.ReadString("SQUIRIX_TEST_ROOT");
            if (!string.IsNullOrWhiteSpace(testRoot))
                return PathEx.Combine(testRoot, clusterId, nodeId);

            var dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            if (string.IsNullOrWhiteSpace(dir) && !OperatingSystem.IsWindows())
                dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);

            return string.IsNullOrWhiteSpace(dir) ? throw new InvalidOperationException(
                    "Cannot determine default data directory: LocalApplicationData is not available. Set PersistenceOptions.DataDir explicitly or define the HOME / XDG_DATA_HOME environment variable.")
                : PathEx.Combine(dir, "squirix", clusterId, nodeId);
        }
    }

    /// <summary>Optional overrides for hosting composition.</summary>
    private sealed class CompositionArgs : ICompositionArgs
    {
        public AdmissionOptions? BackpressureOptions { get; set; }

        public Action<GrpcServiceOptions>? ConfigureGrpc { get; set; }

        public ExtensionOptions? Extensions { get; set; }

        public bool FoundationOnly { get; set; }

        public PressureOptions? MemoryPressureOptions { get; set; }

        public MtlsCertificateMaterial? MtlsMaterial { get; set; }

        public MtlsOptions? MtlsOptions { get; set; }

        public Func<string, HttpMessageHandler>? PeerHandlerFactory { get; set; }

        public PersistenceOptions? PersistenceOptions { get; set; }

        public SecurityOptions? SecurityOptions { get; set; }

        public Action<IServiceCollection>? ServicesConfigure { get; set; }

        public bool WaitForRecovery { get; set; } = true;
    }
}
