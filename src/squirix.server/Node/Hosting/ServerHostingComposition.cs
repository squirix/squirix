using System;
using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
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
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Hosting;

internal static class ServerHostingComposition
{
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

    /// <summary>Opens node storage on the built application, then maps middleware and endpoints.</summary>
    /// <param name="app">The built application.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The supplied application.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the application was already mapped.</exception>
    internal static async Task<WebApplication> MapServerAsync(WebApplication app, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(app);

        var options = app.Services.GetRequiredService<SquirixServerEndpointMappingOptions>();
        if (!options.TryMarkMapped())
            throw new InvalidOperationException("MapSquirixServerAsync was already called for this application.");

        await OpenStorageAsync(app.Services, cancellationToken).ConfigureAwait(false);

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

        if (!options.AuthEnabled)
            return MapEndpoints(app, options.AuthEnabled);
        _ = app.UseAuthentication();
        _ = app.UseAuthorization();

        return MapEndpoints(app, options.AuthEnabled);
    }

    /// <summary>Registers the replica group registry and replication services for an activated node.</summary>
    /// <param name="services">DI service collection.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="persistence">Resolved persistence options.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <remarks>
    /// One group per owner whose replica group includes this node (its own group among them); other groups are not opened.
    /// The logs open in <see cref="OpenStorageAsync" />, so any replication RPC fails closed until its log is ready.
    /// </remarks>
    private static void AddReplicaGroupRegistry(IServiceCollection services, TopologyOptions cluster, PersistenceOptions persistence, MtlsOptions mtlsOptions)
    {
        var activation = new ReplicaGroupActivation([.. TopologyFingerprint.CreateFromTopology(cluster, mtlsOptions).Bytes]);
        _ = services.AddSingleton(activation);

        var peerIds = new string[cluster.Peers.Count];
        for (var i = 0; i < peerIds.Length; i++)
            peerIds[i] = cluster.Peers[i].NodeId;

        _ = services.AddSingleton(sp => CreateReplicaGroupRegistry(sp, cluster, persistence.DataDir, peerIds, activation));

        AddReplicaGroupAppliers(services);
        _ = services.AddSingleton<IGroupLeaderTable>(static sp => new ReplicaLeaderTable(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));

        // Factory registrations let the container own disposal: the registry closes follower-log durability workers
        // and the committers drain their coordinators on host shutdown.
        _ = services.AddSingleton(static sp => CreateReplicaGroupCommitters(sp, sp.GetRequiredService<ReplicaGroupActivation>().Fingerprint));
        _ = services.AddHostedService(static sp => new ReplicaGroupReadinessService(
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            sp.GetRequiredService<ILogger<ReplicaGroupReadinessService>>(),
            sp.GetService<TimeProvider>() ?? TimeProvider.System,
            sp.GetRequiredService<ReplicaCatchUpMetrics>()));
        _ = services.AddHostedService(static sp => new ReplicaApplyService(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<ReplicaGroupAppliers>(),
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            sp.GetRequiredService<IJournalCoordinator>(),
            sp.GetRequiredService<ILogger<ReplicaApplyService>>(),
            sp.GetService<TimeProvider>() ?? TimeProvider.System));
        _ = services.AddSingleton(new ReplicaLogCompactionOptions());
        _ = services.AddHostedService(static sp => new ReplicaLogCompactionService(
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            sp.GetRequiredService<IJournalCoordinator>(),
            sp.GetRequiredService<ReplicaLogCompactionOptions>(),
            ReplicaLogCompactionPolicy.From(sp.GetRequiredService<PersistenceOptions>()),
            sp.GetRequiredService<ReplicationMetrics>(),
            sp.GetRequiredService<ReplicaGroupAppliers>(),
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<ILogger<ReplicaLogCompactionService>>(),
            sp.GetService<TimeProvider>() ?? TimeProvider.System));
        AddReplicaExpirationSweep(services);

        // Network replication is activated here; automatic failover adds the election drivers, which run for groups of three or more.
        if (cluster.AutomaticFailoverEnabled)
            AddReplicaElection(services);
        _ = services.AddSingleton<IReplicaStatusSource>(static sp => new ReplicaGroupStatusSource(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddHealthChecks().Add(
            new HealthCheckRegistration(
                "replica_readiness",
                static sp => new ReplicaReadinessHealthCheck(sp.GetRequiredService<IReplicaStatusSource>(), sp.GetRequiredService<ReplicationMetrics>()),
                HealthStatus.Unhealthy,
                ["ready"]));
    }

    /// <summary>Creates the registry of the groups this node serves, with the election timing and clock of the host.</summary>
    /// <param name="sp">The service provider.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="dataDir">Exclusive node data directory.</param>
    /// <param name="peerIds">The identifiers of every configured node.</param>
    /// <param name="activation">The activated topology fingerprint.</param>
    /// <returns>The registry; its logs open in <see cref="OpenStorageAsync" />.</returns>
    private static ReplicaGroupRegistry CreateReplicaGroupRegistry(IServiceProvider sp, TopologyOptions cluster, string dataDir, string[] peerIds, ReplicaGroupActivation activation) => new(
        dataDir,
        ReplicaGroupMembership.GroupsServedBy(sp.GetRequiredService<IReplicaGroupLocator>(), peerIds, cluster.NodeId),
        cluster.ReplicaCount,
        activation.Fingerprint.AsMemory(),
        cluster.ConfigurationGeneration,
        sp.GetRequiredService<ILoggerFactory>(),
        ReplicaGroupLogOptions(sp))
    {
        Election = sp.GetService<ElectionTimerOptions>() ?? new ElectionTimerOptions(),
        ElectionClock = sp.GetService<TimeProvider>() ?? TimeProvider.System,
    };

    /// <summary>Registers the appliers of every served group, which the committers of the led groups and the apply loops of the others drive.</summary>
    /// <param name="services">DI service collection.</param>
    private static void AddReplicaGroupAppliers(IServiceCollection services) =>
        _ = services.AddSingleton(static sp => new ReplicaGroupAppliers(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey),
            sp.GetRequiredService<TopologyOptions>().NodeId,
            sp.GetRequiredService<ILogger<ReplicaGroupAppliers>>(),
            sp.GetRequiredService<ReplicationMetrics>()));

    /// <summary>Registers the election drivers of the served groups, which hand won terms to the committers.</summary>
    /// <param name="services">DI service collection.</param>
    private static void AddReplicaElection(IServiceCollection services) =>
        _ = services.AddHostedService(static sp => new ReplicaElectionService(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<IReplicaGroupLocator>(),
            sp.GetRequiredService<IReplicaVoteGateway>(),
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            (sp.GetRequiredService<ReplicaGroupActivation>().Fingerprint.AsMemory(), sp.GetRequiredService<TopologyOptions>().ConfigurationGeneration, sp.GetRequiredService<TopologyOptions>().NodeId),
            sp.GetRequiredService<ILogger<ReplicaElectionService>>()));

    /// <summary>Registers the sweep that expires the keys of the led groups no read touches, through committed tombstones.</summary>
    /// <param name="services">DI service collection.</param>
    private static void AddReplicaExpirationSweep(IServiceCollection services) =>
        _ = services.AddHostedService(static sp => new ReplicaExpirationSweepService(
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            sp.GetRequiredService<ILocalCacheSnapshotReader<object?>>(),
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<ILogger<ReplicaExpirationSweepService>>()));

    /// <summary>Returns the settings of the replica group logs: the idempotency window of the replication policy and the host clock.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The follower log settings.</returns>
    /// <remarks>The policy values are hashed into the topology fingerprint, so every member of a group keeps outcomes for the same window.</remarks>
    private static FollowerLogOptions ReplicaGroupLogOptions(IServiceProvider sp) => new()
    {
        IdempotencyCapacity = PolicyOptions.RfIdempotencyMaxInFlightRecords,
        IdempotencyRetention = TimeSpan.FromTicks(PolicyOptions.RfIdempotencyRetentionTicks),
        TimeProvider = sp.GetService<TimeProvider>(),
    };

    /// <summary>Creates the committers of the groups this node leads: the group it owns statically, or the groups the election hands it.</summary>
    /// <param name="sp">The service provider.</param>
    /// <param name="fingerprint">The static topology fingerprint.</param>
    /// <returns>The committers, which own the disposal of each committer.</returns>
    private static ReplicaGroupCommitters CreateReplicaGroupCommitters(IServiceProvider sp, ImmutableArray<byte> fingerprint)
    {
        var topology = sp.GetRequiredService<TopologyOptions>();
        var owners = sp.GetRequiredService<INodeLocator>();
        var clock = sp.GetService<TimeProvider>() ?? TimeProvider.System;
        if (!LeadsByElection(topology))
        {
            var led = new ReplicaGroupCommitter[1];
            led[0] = CreateCommitter(sp, fingerprint, topology.NodeId, null);
            return new ReplicaGroupCommitters(led, topology.NodeId, owners, clock);
        }

        var registry = sp.GetRequiredService<ReplicaGroupRegistry>();
        return new ReplicaGroupCommitters(
            groupId => CreateCommitter(sp, fingerprint, groupId, registry.StateFor(groupId)),
            sp.GetRequiredService<IGroupLeaderTable>(),
            topology.NodeId,
            owners,
            clock);
    }

    /// <summary>Creates the committer of a group this node leads.</summary>
    /// <param name="sp">The service provider.</param>
    /// <param name="fingerprint">The static topology fingerprint.</param>
    /// <param name="groupId">The led group.</param>
    /// <param name="election">The election state the committer leads by; <see langword="null" /> for the own group led statically.</param>
    /// <returns>The committer.</returns>
    private static ReplicaGroupCommitter CreateCommitter(IServiceProvider sp, ImmutableArray<byte> fingerprint, string groupId, ReplicaGroupState? election)
    {
        var topology = sp.GetRequiredService<TopologyOptions>();
        return new ReplicaGroupCommitter(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<IReplicaGroupLocator>(),
            sp.GetRequiredService<IReplicaRpcGateway>(),
            sp.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey),
            (groupId, topology.NodeId),
            new ReplicaTopologyStamp(fingerprint.AsMemory(), topology.ConfigurationGeneration),
            sp.GetRequiredService<ILogger<ReplicaGroupCommitter>>())
        {
            Recovery = sp.GetRequiredService<IJournalCoordinator>(),
            Applier = sp.GetRequiredService<ReplicaGroupAppliers>().For(groupId),
            Clock = sp.GetService<TimeProvider>() ?? TimeProvider.System,
            Election = election,

            // A led group waits at most one maintenance interval for its followers and its commit gate, so a stalled group holds back no other.
            CompactionWaitBudget = sp.GetRequiredService<ReplicaLogCompactionOptions>().Interval,
        };
    }

    /// <summary>Tells whether the groups of this node are led by election: automatic failover on and at least three replicas per group.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <returns><see langword="true" /> when an election driver leads every group; otherwise the owner leads its group statically.</returns>
    private static bool LeadsByElection(TopologyOptions cluster) => cluster.AutomaticFailoverEnabled && cluster.ReplicaCount >= 3;

    /// <summary>
    /// Registers cluster locator, internode transport, and replication planning services.
    /// Composition root for Cluster child namespaces (parent Cluster must not reference them).
    /// </summary>
    /// <remarks>
    /// The repair service stays registered on every node, including RF=1 and foundation-only hosts: the idle
    /// service parks on its queue read without burning a thread.
    /// </remarks>
    /// <param name="services">DI service collection.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="args">Hosting composition overrides including optional peer handler factory.</param>
    private static void AddSquirixClusterStack(IServiceCollection services, TopologyOptions cluster, ICompositionArgs args)
    {
        _ = services.AddSquirixClusterLocator(cluster);
        _ = services.AddHealthChecks().Add(
            new HealthCheckRegistration(
                "ring_agreement",
                static sp => new RingAgreementHealthCheck(sp.GetRequiredService<RingAgreement>()),
                HealthStatus.Unhealthy,
                ["ready"]));
        _ = services.AddSquirixClusterTransport(cluster, null, args.PeerHandlerFactory, (args.BackpressureOptions ?? new AdmissionOptions()).MaxInFlight);
        _ = services.AddSquirixClusterReplication(cluster, args.FoundationOnly);
        if (!args.FoundationOnly && cluster.ReplicaCount <= 1)
            return;
        _ = services.AddSingleton(static sp => new SquirixReplicationServiceAdapter(
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<MtlsCertificate>(),
            sp.GetService<ReplicaGroupRegistry>()));
        _ = services.AddSingleton(static sp => new ReplicaRpcGateway(sp.GetRequiredService<IServerClientPool>()));
        _ = services.AddSingleton<IReplicaRpcGateway>(static sp => sp.GetRequiredService<ReplicaRpcGateway>());
        _ = services.AddSingleton<IReplicaVoteGateway>(static sp => sp.GetRequiredService<ReplicaRpcGateway>());
    }

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
                Certificate = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);

        // Per-host Meter. A single instance is created here, then both the runtime and the persistence composition
        // resolve this same singleton. It is registered through the factory overload, so the DI container takes
        // ownership and disposes of it when the owning host shuts down: AddSingleton(instance) does not transfer
        // disposal ownership in Microsoft DI, which would leak the meter.
        Meter? ownedMeter = null;
        Meter serverMeter;
        try
        {
            ownedMeter = new Meter("Squirix");
            serverMeter = ownedMeter;
            _ = builder.Services.AddSingleton(_ => serverMeter);
            ownedMeter = null;
        }
        finally
        {
            ownedMeter?.Dispose();
        }

        _ = builder.Services.AddSquirixRuntimeServices();
        AddSquirixClusterStack(builder.Services, cluster, args);
        RegisterPersistenceAndReplication(builder.Services, cluster, persistence, serverMeter, mtlsOptions, args);

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
    /// <exception cref="InvalidOperationException">
    /// Thrown when the configured identity differs from the stamped one, or when an unstamped RF&gt;1 directory already holds RF=1 journal state.
    /// </exception>
    /// <remarks>
    /// Nothing rewrites the stamp after first activation, so an RF&gt;1 restart with a changed topology fails startup
    /// instead of splitting the replica set, and <see cref="EnsureNotActivatedAsync" /> refuses an RF=1 start on a
    /// stamped directory. Both checks run before storage opens. Migrating an existing data directory to a different activated
    /// topology, including RF=1 to RF&gt;1 and RF&gt;1 to RF=1, is not supported in this release.
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
            // A truly empty data directory is a first activation: record the configured identity and proceed.
            // Only RF>1 activation writes the stamp, so a directory that already carries durable cache journal
            // state belonged to an RF=1 node. Stamping it as RF>1 would adopt data no replica group holds, and
            // moving RF=1 data to RF>1 is not supported, so refuse startup.
            if (replicaCount > 1 && ActivatedTopologyStampStore.HasDurableCacheJournalState(dataDir))
            {
                throw new InvalidOperationException(
                    "Data directory holds durable cache journal state but no activated topology stamp, so it was last used by an RF=1 node; " +
                    "moving existing RF=1 data to RF>1 is not supported in this release. " +
                    "Start the RF>1 node on an empty data directory, or migrate the data at the application level.");
            }

            await store.PublishAsync(current, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!stamped.Matches(current))
        {
            throw new InvalidOperationException(
                $"Configured topology does not match the activated topology stamp in the data directory: {stamped.DescribeChange(current)}. " +
                "Changing the activated topology of an existing data directory is not supported in this release; " +
                "start the node with the configuration and package version the directory was activated with, or on an empty data directory.");
        }
    }

    /// <summary>Refuses an RF=1 start on a data directory activated for replication.</summary>
    /// <param name="dataDir">Exclusive node data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the directory carries no activated topology stamp.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the directory carries an activated topology stamp.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown when the stamp is corrupt or unsupported.</exception>
    /// <remarks>
    /// Only RF&gt;1 activation writes the stamp, so any stamp means the directory holds replica group logs that an
    /// RF=1 node would silently ignore. Runs before storage opens.
    /// </remarks>
    private static async Task EnsureNotActivatedAsync(string dataDir, CancellationToken cancellationToken)
    {
        var stamped = await new ActivatedTopologyStampStore(dataDir).ReadAsync(cancellationToken).ConfigureAwait(false);
        if (stamped == null)
            return;

        throw new InvalidOperationException(
            $"Data directory was activated for replica count {stamped.ReplicaCount.ToString(CultureInfo.InvariantCulture)} " +
            $"(generation {stamped.Generation.ToString(CultureInfo.InvariantCulture)}); starting it as RF=1 is not supported in this release. " +
            "Start the node with the replica count the directory was activated with, or on an empty data directory.");
    }

    /// <summary>Checks the activated-topology stamp, then opens persistence and the replica group logs in the order that preserves container disposal order.</summary>
    /// <param name="services">The built service provider.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every storage component is open.</returns>
    /// <remarks>
    /// The topology stamp is checked before persistence opens, so a directory the node refuses is never repaired or otherwise modified.
    /// The registry is resolved first so the container disposes it last, after the journal host and the ledger.
    /// Nothing here is undone on failure: the registry closes the logs it opened partially, and the container disposes every component it created, including the registry, when the application is disposed.
    /// </remarks>
    private static async Task OpenStorageAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        var persistence = services.GetService<PersistenceOptions>();
        if (persistence == null)
            return;

        var registry = services.GetService<ReplicaGroupRegistry>();
        var cluster = services.GetRequiredService<TopologyOptions>();
        if (cluster.ReplicaCount <= 1)
        {
            await EnsureNotActivatedAsync(persistence.DataDir, cancellationToken).ConfigureAwait(false);
        }
        else if (registry != null)
        {
            var activation = services.GetRequiredService<ReplicaGroupActivation>();
            await EnsureActivatedTopologyAsync(persistence.DataDir, activation.Fingerprint.AsMemory(), cluster.ConfigurationGeneration, cluster.ReplicaCount, cancellationToken)
               .ConfigureAwait(false);
        }

        await PersistenceServiceRegistration.OpenPersistenceAsync(services, cancellationToken).ConfigureAwait(false);
        if (registry == null)
            return;

        await registry.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    private static WebApplication MapEndpoints(WebApplication app, bool authEnabled)
    {
        _ = app.MapSquirixEndpoints(authEnabled);
        var extensions = app.Services.GetService<ExtensionOptions>();
        extensions?.MapEndpoints?.Invoke(app);
        extensions?.MapEndpointsWithAuthorization?.Invoke(app, authEnabled);
        return app;
    }

    /// <summary>Registers persistence, follower-group storage, and the replica group registry on the service collection.</summary>
    /// <param name="services">DI service collection.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="persistence">Resolved persistence options; <see langword="null" /> when persistence is disabled.</param>
    /// <param name="serverMeter">The per-host Meter singleton owned by the container.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <param name="args">Composition arguments.</param>
    private static void RegisterPersistenceAndReplication(
        IServiceCollection services,
        TopologyOptions cluster,
        PersistenceOptions? persistence,
        Meter serverMeter,
        MtlsOptions mtlsOptions,
        ICompositionArgs args)
    {
        if (persistence == null)
            return;

        _ = services.AddPersistenceServices(persistence, serverMeter, args.WaitForRecovery);

        if (cluster.ReplicaCount > 1 && !args.FoundationOnly)
            AddReplicaGroupRegistry(services, cluster, persistence, mtlsOptions);
    }

    private static (MtlsOptions Options, MtlsCertificate Material) ResolveClusterTransportSecurity(
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
        ReplicationActivationGuard.ThrowIfDisallowed(cluster.ReplicaCount, persistenceEnabled, mtlsOptions, cluster.ReplicationEnabled);

        var certificate = KestrelConfiguration.ConfigureKestrel(builder, uri, cluster, mtlsOptions, args.Certificate, requiresInterNodeMtls);
        return (mtlsOptions, certificate);
    }

    [Immutable]
    private sealed record ReplicaGroupActivation(ImmutableArray<byte> Fingerprint);

    /// <summary>
    /// Centralizes Kestrel listen options and transport security for the squirix node process.
    /// Invariants here affect TLS listener setup — review carefully.
    /// </summary>
    private static class KestrelConfiguration
    {
        /// <summary>Resolves the cluster mTLS certificate material and configures Kestrel listeners from it.</summary>
        /// <param name="builder">The web application builder.</param>
        /// <param name="uri">The primary HTTPS listen URI.</param>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="mtlsOptions">Cluster mTLS options.</param>
        /// <param name="suppliedCertificate">Caller-supplied certificate material, when the caller owns loading it; otherwise <see langword="null" /> to load it here.</param>
        /// <param name="requiresInterNodeMtls">Whether internode mTLS transport is required for this topology.</param>
        /// <returns>The certificate material Kestrel was configured with.</returns>
        internal static MtlsCertificate ConfigureKestrel(
            WebApplicationBuilder builder,
            Uri uri,
            TopologyOptions cluster,
            MtlsOptions mtlsOptions,
            MtlsCertificate? suppliedCertificate,
            bool requiresInterNodeMtls)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(uri);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(mtlsOptions);

            if (suppliedCertificate != null)
            {
                ConfigureKestrelListeners(builder, uri, cluster, mtlsOptions, suppliedCertificate);
                return suppliedCertificate;
            }

            MtlsCertificate? loaded = null;
            try
            {
                loaded = MtlsCertificate.Load(mtlsOptions, uri.Port, requiresInterNodeMtls, cluster.NodeId);
                ConfigureKestrelListeners(builder, uri, cluster, mtlsOptions, loaded);
                var certificate = loaded;
                loaded = null;
                return certificate;
            }
            finally
            {
                (loaded as IDisposable)?.Dispose();
            }
        }

        /// <summary>Ensures the node URI uses HTTPS gRPC transport.</summary>
        /// <param name="cluster">Cluster configuration including the node URI.</param>
        /// <exception cref="InvalidOperationException">Thrown when the node URI uses plaintext HTTP.</exception>
        internal static void EnsureHttpsTransport(TopologyOptions cluster)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            if (!cluster.Uri.IsAbsoluteUri || !cluster.Uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Squirix transport requires HTTPS. Plaintext HTTP is not supported.");
        }

        private static void ConfigureKestrelListeners(WebApplicationBuilder builder, Uri uri, TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificate certificate)
        {
            var mtlsEnabled = certificate.Enabled;
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
                    kestrel.ListenLocalhost(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, certificate, remotePeerNodeIds));
                else
                    kestrel.ListenAnyIP(mtlsOptions.InternalListenPort, listenOptions => ConfigureMtlsEndpoint(listenOptions, certificate, remotePeerNodeIds));
            });
        }

        private static void ConfigureMtlsEndpoint(ListenOptions listenOptions, MtlsCertificate material, string[] nodeIds)
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
            _ = listenOptions.UseHttps(https => ConfigureMutualTls(https, material, nodeIds));
        }

        private static void ConfigureMutualTls(HttpsConnectionAdapterOptions https, MtlsCertificate material, string[] remotePeerNodeIds)
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

            var dataDir = string.IsNullOrWhiteSpace(source.DataDir) ? DefaultDataDirectory.Resolve(cluster.ClusterId, cluster.NodeId) : source.DataDir;
            return source with { DataDir = dataDir };
        }
    }

    /// <summary>Optional overrides for hosting composition.</summary>
    private sealed class CompositionArgs : ICompositionArgs
    {
        public AdmissionOptions? BackpressureOptions { get; set; }

        public MtlsCertificate? Certificate { get; set; }

        public Action<GrpcServiceOptions>? ConfigureGrpc { get; set; }

        public ExtensionOptions? Extensions { get; set; }

        public bool FoundationOnly { get; set; }

        public PressureOptions? MemoryPressureOptions { get; set; }

        public MtlsOptions? MtlsOptions { get; set; }

        public Func<string, HttpMessageHandler>? PeerHandlerFactory { get; set; }

        public PersistenceOptions? PersistenceOptions { get; set; }

        public SecurityOptions? SecurityOptions { get; set; }

        public Action<IServiceCollection>? ServicesConfigure { get; set; }

        public bool WaitForRecovery { get; set; } = true;
    }

    /// <summary>Host authentication state for endpoint mapping, plus the once-only guard for <see cref="MapServerAsync" />.</summary>
    [ThreadSafe]
    private sealed class SquirixServerEndpointMappingOptions
    {
        private int _mapped;

        internal SquirixServerEndpointMappingOptions(bool authEnabled)
        {
            AuthEnabled = authEnabled;
        }

        /// <summary>Gets a value indicating whether data-plane authentication is enabled.</summary>
        internal bool AuthEnabled { get; }

        /// <summary>Marks the application as mapped.</summary>
        /// <returns><see langword="true" /> for the first caller; <see langword="false" /> when the application was already mapped.</returns>
        internal bool TryMarkMapped() => Interlocked.Exchange(ref _mapped, 1) == 0;
    }
}
