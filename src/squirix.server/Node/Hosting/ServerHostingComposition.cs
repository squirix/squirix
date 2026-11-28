using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Endpoint.Resp;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Logging;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Node.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class ServerHostingComposition
{
    internal const string MetricsMeterName = "Squirix";

    /// <summary>Configures node services from cluster topology and optional composition overrides.</summary>
    /// <param name="services">The server services.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="configure">Optional composition overrides callback.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when builder configuration finishes.</returns>
    internal static Task ConfigureBuilderAsync(
        NodeServices services,
        TopologyOptions cluster,
        Action<CompositionArgs>? configure = null,
        CancellationToken cancellationToken = default)
    {
        var args = new CompositionArgs();
        configure?.Invoke(args);
        return ConfigureBuilderCoreAsync(services, cluster, args, cancellationToken);
    }

    /// <summary>Locks the data directory, checks the activated-topology stamp, then opens persistence and the replica group logs in the order that preserves container disposal order.</summary>
    /// <param name="services">The built service provider.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every storage component is open.</returns>
    /// <remarks>
    /// The data directory lock is taken first, so a directory held by another node is refused before anything reads or writes it; the only things changed before the stamp check are the directory, its lock file, and the modes of the node's own entries in it.
    /// The topology stamp is checked before persistence opens, so a directory the node refuses is never repaired or otherwise modified.
    /// The lock is resolved first so the container disposes it last, after the registry, the journal host and the ledger.
    /// The registry is resolved next so the container disposes it after the journal host and the ledger.
    /// Nothing here is undone on failure: the registry closes the logs it opened partially, and the container disposes every component it created, including the registry, when the application is disposed.
    /// </remarks>
    internal static async Task OpenStorageAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        var persistence = services.Find<PersistenceOptions>();
        if (persistence == null)
            return;

        // Resolved first so the container disposes the lock last, and so no other process touches the directory from here on.
        _ = services.Require<DataDirectoryLock>();
        var registry = services.Find<ReplicaGroupRegistry>();
        var cluster = services.Require<TopologyOptions>();
        if (cluster.ReplicaCount <= 1)
        {
            await EnsureNotActivatedAsync(persistence.DataDir, cancellationToken).ConfigureAwait(false);
        }
        else if (registry != null)
        {
            var activation = services.Require<ReplicaGroupActivation>();
            await EnsureActivatedTopologyAsync(
                persistence.DataDir,
                activation.FingerprintBytes.AsMemory(),
                cluster.ConfigurationGeneration,
                cluster.ReplicaCount,
                cancellationToken).ConfigureAwait(false);
        }

        await PersistenceServiceRegistration.OpenPersistenceAsync(services, cancellationToken).ConfigureAwait(false);
        if (registry == null)
            return;

        await registry.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Registers cluster locator, internode transport, and replication planning services.
    /// Composition root for Cluster child namespaces (parent Cluster must not reference them).
    /// </summary>
    /// <remarks>
    /// The repair service stays registered on every node, including RF=1 and foundation-only hosts: the idle
    /// service parks on its queue read without burning a thread.
    /// Every host that is not network-replication-activated, with or without persistence, also gets the local expiry sweep, since
    /// its local clock decides expiry.
    /// </remarks>
    /// <param name="services">Node component registrations.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="args">Hosting composition overrides for this node.</param>
    private static void AddSquirixClusterStack(NodeServices services, TopologyOptions cluster, CompositionArgs args)
    {
        var feature = FeatureState.Resolve(cluster, args.FoundationOnly);
        _ = services.AddSquirixClusterLocator(cluster, EntryLimits.ValueBytes(feature.NetworkReplicationEnabled));
        _ = services.Use(new ReadinessCheck("ring_agreement", static sp => new RingAgreementHealthCheck(sp.Require<RingAgreement>())));
        _ = services.AddSquirixClusterTransport(cluster, null, (args.BackpressureOptions ?? new AdmissionOptions()).MaxInFlight);
        _ = services.AddSquirixClusterReplication(cluster, args.FoundationOnly);

        // Activated hosts expire entries through committed records and the replica sweep; every other host expires by its local clock.
        if (feature != FeatureState.Activated)
        {
            _ = services.RunService(static sp => new LocalExpirationSweepService(
                sp.Require<ILocalCacheExpiredKeys>(),
                sp.Require<TimeProvider>(),
                sp.Require<IServerLoggerFactory>().CreateLogger<LocalExpirationSweepService>()));
        }

        if (args.Eviction?.EvictOnMemoryPressure == true)
        {
            _ = services.RunService(sp => new MemoryEvictionService(
                sp.Require<PhysicalCache<StoredValue>>(),
                sp.Require<IMemoryPressureStateEvaluator>(),
                sp.Require<OwnerLocalCache>().Value,
                feature.NetworkReplicationEnabled ? sp.Require<ReplicaGroupCommitters>() : null,
                sp.Require<TimeProvider>(),
                sp.Require<IServerLoggerFactory>().CreateLogger<MemoryEvictionService>()));
        }

        if (!args.FoundationOnly && cluster.ReplicaCount <= 1)
            return;
        _ = services.Own(static sp => new SquirixReplicationServiceAdapter(
            sp.Require<TopologyOptions>(),
            sp.Require<MtlsOptions>(),
            sp.Find<VerifiedPeerCertificates>(),
            sp.Find<ReplicaGroupRegistry>(),
            sp.Find<IReplicaMembership>(),
            sp.Find<ReplicaTransferReceiver>(),
            sp.Find<ICriticalMemoryPressureReader>()));
        _ = services.Own(static sp => new ReplicaRpcGateway(sp.Require<IServerClientPool>()));
        _ = services.Own<IReplicaRpcGateway>(static sp => sp.Require<ReplicaRpcGateway>());
        _ = services.Own<IReplicaVoteGateway>(static sp => sp.Require<ReplicaRpcGateway>());
        _ = services.Own<IReplicaBootstrapGateway>(static sp => new ReplicaBootstrapRpcGateway(sp.Require<IServerClientPool>()));
        _ = services.Own<IReplicaTransferGateway>(static sp => new ReplicaTransferRpcGateway(sp.Require<IServerClientPool>()));
    }

    private static async Task ConfigureBuilderCoreAsync(NodeServices services, TopologyOptions cluster, CompositionArgs args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(args);

        var replicated = FeatureState.Resolve(cluster, args.FoundationOnly).NetworkReplicationEnabled;
        var persistence = args.PersistenceOptions == null ? null : PersistenceOptionsResolver.Resolve(cluster, args.PersistenceOptions, replicated);
        var persistenceEnabled = persistence != null;
        var pressure = args.MemoryPressureOptions ?? OptionsResolver.Resolve(
            await PressureBootstrap.LoadAsync(cancellationToken, args.SettingsFilePath).ConfigureAwait(false), GcMemoryBudgetProvider.Instance);
        if (args.MemoryAccountingEnabled is { } enabled)
            pressure = pressure with { MemoryAccountingEnabled = enabled };
        PressureBootstrap.ValidateEviction(args.Eviction, pressure.MemoryAccountingEnabled);

        var (mtlsOptions, mtlsMaterial) = ResolveClusterTransportSecurity(cluster, args, persistenceEnabled);

        _ = await services.AddSquirixValidatedOptionsAsync(
            cluster,
            new ValidatedOptionsArgs
            {
                SettingsFilePath = args.SettingsFilePath,
                BackpressureOptions = args.BackpressureOptions,
                PersistenceOptions = persistence,
                MemoryPressureOptions = pressure,
                PrometheusMetricsOptions = args.PrometheusMetricsOptions,
                MtlsOptions = mtlsOptions,
                Certificate = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);

        RegisterRuntimeServices(services, args, pressure.MemoryAccountingEnabled, pressure.MemoryAccountingEnabled && !persistenceEnabled && !replicated ? pressure.CriticalBytes : null);
        AddSquirixClusterStack(services, cluster, args);
        RegisterPersistenceAndReplication(services, cluster, persistence, mtlsOptions, args);

        RegisterCacheEndpoints(services, cluster, args, persistenceEnabled, pressure.MemoryAccountingEnabled && (persistenceEnabled || replicated));
    }

    private static void RegisterCacheEndpoints(NodeServices services, TopologyOptions cluster, CompositionArgs args, bool persistenceEnabled, bool admitBeforeCommit)
    {
        // A cluster owner can receive RESP operations through a peer while its own RESP listener is disabled.
        var orderedReadCacheName = (args.RespUri != null || cluster.Peers.Count > 1) ? CacheCommands.CacheName : null;
        _ = services.AddSquirixCachePipeline(persistenceEnabled, args.MetricsEnabled, admitBeforeCommit, orderedReadCacheName);
        _ = services.AddSquirixNodeEndpointServices(persistenceEnabled);
        var authEnabled = services.AddSquirixSecurityServices(args.SecurityOptions);
        ExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(cluster.Uri, authEnabled);
        if (args.RespUri is { } respUri)
            ExternalAccessSecurity.EnsureDataPlaneAuthenticatedForListenUri(respUri, authEnabled);
        _ = services.AddSquirixFrameworkServices();
        _ = services.Own(static sp => new LargeCacheRequests(
            sp.Require<FeatureState>().NetworkReplicationEnabled,
            sp.Require<PressureOptions>().MaxEstimatedCacheBytes));
        _ = services.Own(sp => new NativeClientService(
            sp,
            cluster,
            FeatureState.Resolve(cluster, args.FoundationOnly).NetworkReplicationEnabled,
            args.ManagementUri,
            args.ServerCertificate,
            args.RespUri));
        _ = services.RunService(static sp => sp.Require<NativeClientService>());
    }

    private static void RegisterRuntimeServices(NodeServices services, CompositionArgs args, bool memoryAccountingEnabled, long? localWriteLimit)
    {
        if (args.MetricsEnabled)
        {
            _ = services.Own(static _ => new Meter(MetricsMeterName));
            _ = services.Own(static sp => new PrometheusMetricsScraper(sp.Require<Meter>()));
        }

        if (args.Eviction != null)
            _ = services.Use(args.Eviction);
        _ = services.AddSquirixRuntimeServices(args.MetricsEnabled, memoryAccountingEnabled, localWriteLimit);
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

    /// <summary>Takes the data directory lock, restricts the entries it already holds to the owner, and warns when the operating system does not enforce the lock.</summary>
    /// <param name="sp">The service provider.</param>
    /// <param name="dataDir">The data directory path.</param>
    /// <returns>The held lock.</returns>
    private static DataDirectoryLock AcquireDataDirectoryLock(IServiceProvider sp, string dataDir)
    {
        var directoryLock = DataDirectoryLock.Acquire(dataDir);
        var logger = sp.Require<IServerLoggerFactory>().CreateLogger<DataDirectoryLock>();

        // Under the lock and before anything opens the files: data written by an earlier version may be readable by other users.
        DataDirectoryPermissions.Outcome tightened;
        try
        {
            tightened = DataDirectoryPermissions.TightenExisting(directoryLock.Directory);
        }
        catch
        {
            directoryLock.Dispose();
            throw;
        }

        if (tightened.Tightened > 0)
            StorageLog.DataDirectoryPermissionsTightened(logger, directoryLock.Directory, tightened.Tightened);

        if (tightened.RootRefused)
            StorageLog.DataDirectoryRootNotRestricted(logger, directoryLock.Directory);

        if (!directoryLock.IsEnforced)
            StorageLog.DataDirectoryLockNotEnforced(logger, directoryLock.Directory);

        return directoryLock;
    }

    /// <summary>Registers persistence, follower-group storage, and the replica group registry on the service collection.</summary>
    /// <param name="services">Node component registrations.</param>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="persistence">Resolved persistence options; <see langword="null" /> when persistence is disabled.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <param name="args">Composition arguments.</param>
    private static void RegisterPersistenceAndReplication(
        NodeServices services,
        TopologyOptions cluster,
        PersistenceOptions? persistence,
        MtlsOptions mtlsOptions,
        CompositionArgs args)
    {
        if (persistence == null)
            return;

        // Registered before every storage component: the container disposes in reverse order of creation, so the lock is released last.
        _ = services.Own(sp => AcquireDataDirectoryLock(sp, persistence.DataDir));
        _ = services.AddPersistenceServices(persistence, args.WaitForRecovery, args.MetricsEnabled);

        if (cluster.ReplicaCount > 1 && !args.FoundationOnly)
            ReplicaGroups.Register(services, cluster, persistence, mtlsOptions);
    }

    private static (MtlsOptions Options, MtlsCertificate Material) ResolveClusterTransportSecurity(TopologyOptions cluster, CompositionArgs args, bool persistenceEnabled)
    {
        var uri = cluster.Uri;
        if (!uri.IsAbsoluteUri || !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Squirix transport requires HTTPS. Plaintext HTTP is not supported.");
        var requiresInterNodeMtls = MtlsTopology.RequiresInterNodeMtls(cluster);
        var mtlsOptions = args.MtlsOptions ?? MtlsOptionsResolver.ResolveFromEnvironment();

        // Refuse an invalid topology while the builder is configured, before the mapped host opens or writes any storage.
        if (!TopologyValidator.TryValidate(cluster, out var topologyFailures))
            throw new InvalidOperationException(string.Join("; ", topologyFailures));

        var groupIdFailures = new List<string>();
        ReplicaGroupIdLimit.CollectFailures(groupIdFailures, cluster);
        if (groupIdFailures.Count > 0)
            throw new InvalidOperationException(string.Join("; ", groupIdFailures));

        ReplicationActivationGuard.ThrowIfDisallowed(cluster.ReplicaCount, persistenceEnabled, mtlsOptions, cluster.ReplicationEnabled);

        var certificate = args.Certificate ?? MtlsCertificate.Load(mtlsOptions, uri.Port, requiresInterNodeMtls, cluster.NodeId);
        return (mtlsOptions, certificate);
    }

    [Immutable]
    private sealed record ReplicaGroupActivation(ImmutableArray<byte> FingerprintBytes);

    private static class ReplicaGroups
    {
        /// <summary>Registers the replica group registry and replication services for an activated node.</summary>
        /// <param name="services">Node component registrations.</param>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="persistence">Resolved persistence options.</param>
        /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
        /// <remarks>
        /// One group per owner whose replica group includes this node (its own group among them); other groups are not opened.
        /// The logs open in <see cref="OpenStorageAsync" />, so any replication RPC fails closed until its log is ready.
        /// </remarks>
        internal static void Register(NodeServices services, TopologyOptions cluster, PersistenceOptions persistence, MtlsOptions mtlsOptions)
        {
            var activation = new ReplicaGroupActivation([.. TopologyFingerprint.CreateFromTopology(cluster, mtlsOptions).Bytes]);
            _ = services.Use(activation);

            _ = services.Own(sp => CreateReplicaGroupRegistry(sp, cluster, persistence.DataDir, activation));

            _ = services.Own<IReplicaMembership>(static sp => new ReplicaMembership(
                sp.Require<IReplicaGroupLocator>(),
                sp.Require<ReplicaGroupRegistry>().GroupIds));

            AddReplicaGroupAppliers(services);

            // The cluster locator registered the static leader table; an election-led node replaces it with the table of its election state.
            if (LeadsByElection(cluster))
            {
                _ = services.ReplaceFactory<IGroupLeaderTable>(static sp => new ReplicaLeaderTable(
                        sp.Require<ReplicaGroupRegistry>(),
                        sp.Require<TopologyOptions>().NodeId,
                        sp.Require<IReplicaGroupLocator>()));
            }

            // Factory registrations let the container own disposal: the registry closes follower-log durability workers
            // and the committers drain their coordinators on host shutdown.
            _ = services.Own(static sp => CreateReplicaGroupCommitters(sp, sp.Require<ReplicaGroupActivation>().FingerprintBytes));
            AddReplicaGroupReadiness(services);
            ReplicaGroupBootstrapRegistration.Add(services);
            _ = services.RunService(static sp => new ReplicaApplyService(
                sp.Require<ReplicaGroupRegistry>(),
                sp.Require<ReplicaGroupAppliers>(),
                sp.Require<ReplicaGroupCommitters>(),
                sp.Require<IJournalCoordinator>(),
                sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaApplyService>(),
                sp.Require<TimeProvider>()));
            _ = services.Use(new ReplicaLogCompactionOptions());
            _ = services.RunService(static sp => new ReplicaLogCompactionService(
                sp.Require<ReplicaGroupCommitters>(),
                sp.Require<IJournalCoordinator>(),
                (sp.Require<ReplicaLogCompactionOptions>(), ReplicaLogCompactionPolicy.From(sp.Require<PersistenceOptions>())),
                sp.Require<ReplicationMetrics>(),
                (sp.Require<ReplicaGroupAppliers>(), sp.Require<ReplicaGroupRegistry>()),
                sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaLogCompactionService>(),
                sp.Require<TimeProvider>()));
            AddReplicaExpirationSweep(services);

            // Network replication is activated here; automatic failover adds the election drivers, which run for groups of three or more.
            if (cluster.AutomaticFailoverEnabled)
                AddReplicaElection(services);
            ReplicaStatusRegistration.Add(services);
        }

        /// <summary>Registers the election drivers of the served groups, which hand won terms to the committers.</summary>
        /// <param name="services">Node component registrations.</param>
        private static void AddReplicaElection(NodeServices services) => _ = services.RunService(static sp =>
        {
            var activation = sp.Require<ReplicaGroupActivation>();
            var topology = sp.Require<TopologyOptions>();
            return new ReplicaElectionService(
                sp.Require<ReplicaGroupRegistry>(),
                sp.Require<IReplicaGroupLocator>(),
                sp.Require<IReplicaVoteGateway>(),
                sp.Require<ReplicaGroupCommitters>(),
                (activation.FingerprintBytes.AsMemory(), topology.ConfigurationGeneration, topology.NodeId),
                sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaElectionService>())
            {
                HostStopping = sp.Require<INodeLifetime>().ApplicationStopping,
            };
        });

        /// <summary>Registers the sweep that expires the keys of the led groups no read touches, through committed tombstones.</summary>
        /// <param name="services">Node component registrations.</param>
        private static void AddReplicaExpirationSweep(NodeServices services) => _ = services.RunService(static sp => new ReplicaExpirationSweepService(
            sp.Require<ReplicaGroupCommitters>(),
            sp.Require<ILocalCacheExpiredKeys>(),
            sp.Require<INodeLocator>(),
            sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaExpirationSweepService>()));

        /// <summary>Registers the appliers of every served group, which the committers of the led groups and the apply loops of the others drive.</summary>
        /// <param name="services">Node component registrations.</param>
        private static void AddReplicaGroupAppliers(NodeServices services) => _ = services.Own(static sp => new ReplicaGroupAppliers(
            sp.Require<ReplicaGroupRegistry>(),
            sp.Require<OwnerLocalCache>().Value,
            sp.Require<TopologyOptions>().NodeId,
            sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaGroupAppliers>(),
            sp.Require<ReplicationMetrics>(),
            sp.Require<INodeLocator>()));

        /// <summary>Registers the verification of the followers of the led groups, with its retry schedule.</summary>
        /// <param name="services">Node component registrations.</param>
        /// <remarks>The readiness service sends the state transfers, so their registrations come with it.</remarks>
        private static void AddReplicaGroupReadiness(NodeServices services)
        {
            ReplicaTransferRegistration.Add(services);
            _ = services.RunService(static sp => new ReplicaGroupReadinessService(
                sp.Require<ReplicaGroupCommitters>(),
                sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaGroupReadinessService>(),
                sp.Require<TimeProvider>(),
                sp.Require<ReplicaCatchUpMetrics>(),
                sp.Require<ReplicaStateTransfers>())
            {
                Options = sp.Find<ReplicaReadinessOptions>() ?? new ReplicaReadinessOptions(),
            });
        }

        /// <summary>Creates the committer of a group this node leads.</summary>
        /// <param name="sp">The service provider.</param>
        /// <param name="fingerprint">The static topology fingerprint.</param>
        /// <param name="groupId">The led group.</param>
        /// <param name="election">The election state the committer leads by; <see langword="null" /> for the own group led statically.</param>
        /// <returns>The committer.</returns>
        private static ReplicaGroupCommitter CreateCommitter(IServiceProvider sp, ImmutableArray<byte> fingerprint, string groupId, ReplicaGroupState? election)
        {
            var topology = sp.Require<TopologyOptions>();
            var physical = sp.Require<PhysicalCache<StoredValue>>();
            return new ReplicaGroupCommitter(
                sp.Require<ReplicaGroupRegistry>(),
                sp.Require<IReplicaGroupLocator>(),
                sp.Require<IReplicaRpcGateway>(),
                sp.Require<OwnerLocalCache>().Value,
                (groupId, topology.NodeId),
                new ReplicaTopologyStamp(fingerprint.AsMemory(), topology.ConfigurationGeneration),
                sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaGroupCommitter>())
            {
                TrackMemberPressure = sp.Require<PressureOptions>().MemoryAccountingEnabled,
                SelectCapacityVictim = physical.HasCapacity ? physical.SelectCapacityVictim : null,
                ReplacementAdmission = sp.Find<ReplacementAdmission<StoredValue>>(),
                Recovery = sp.Require<IJournalCoordinator>(),
                Applier = sp.Require<ReplicaGroupAppliers>().For(groupId),
                Clock = sp.Require<TimeProvider>(),

                // The commit and shutdown budgets run on the system clock, so a host clock that never advances cannot park a commit or a dispose.
                BudgetTimeProvider = TimeProvider.System,
                ShutdownTimeProvider = TimeProvider.System,
                Election = election,
                Metrics = sp.Require<ReplicationMetrics>(),

                // A led group waits at most one maintenance interval for its followers and its commit gate, so a stalled group holds back no other.
                CompactionWaitBudget = sp.Require<ReplicaLogCompactionOptions>().Interval,
            };
        }

        /// <summary>Creates the committers of the groups this node leads: the group it owns statically, or the groups the election hands it.</summary>
        /// <param name="sp">The service provider.</param>
        /// <param name="fingerprint">The static topology fingerprint.</param>
        /// <returns>The committers, which own the disposal of each committer.</returns>
        private static ReplicaGroupCommitters CreateReplicaGroupCommitters(IServiceProvider sp, ImmutableArray<byte> fingerprint)
        {
            var topology = sp.Require<TopologyOptions>();
            var owners = sp.Require<INodeLocator>();
            var clock = sp.Require<TimeProvider>();
            if (!LeadsByElection(topology))
            {
                var led = new ReplicaGroupCommitter[1];
                led[0] = CreateCommitter(sp, fingerprint, topology.NodeId, null);
                return new ReplicaGroupCommitters(led, topology.NodeId, owners, clock);
            }

            var registry = sp.Require<ReplicaGroupRegistry>();
            return new ReplicaGroupCommitters(
                groupId => CreateCommitter(sp, fingerprint, groupId, registry.StateFor(groupId)),
                sp.Require<IGroupLeaderTable>(),
                topology.NodeId,
                owners,
                clock)
            {
                // The receiver is resolved when a leadership starts: it depends on the committers through the state sink.
                BeforeLeading = (groupId, cancellationToken) => sp.Require<ReplicaTransferReceiver>().ReleaseAsync(groupId, cancellationToken),
            };
        }

        /// <summary>Creates the registry of the groups this node serves, with the election timing and clock of the host.</summary>
        /// <param name="sp">The service provider.</param>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="dataDir">Exclusive node data directory.</param>
        /// <param name="activation">The activated topology fingerprint.</param>
        /// <returns>The registry; its logs open in <see cref="OpenStorageAsync" />.</returns>
        private static ReplicaGroupRegistry CreateReplicaGroupRegistry(IServiceProvider sp, TopologyOptions cluster, string dataDir, ReplicaGroupActivation activation)
        {
            var peerIds = new string[cluster.Peers.Count];
            for (var i = 0; i < peerIds.Length; i++)
                peerIds[i] = cluster.Peers[i].NodeId;

            return new ReplicaGroupRegistry(
                dataDir,
                ReplicaGroupMembership.GroupsServedBy(sp.Require<IReplicaGroupLocator>(), peerIds, cluster.NodeId),
                cluster.ReplicaCount,
                activation.FingerprintBytes.AsMemory(),
                cluster.ConfigurationGeneration,
                sp.Require<IServerLoggerFactory>(),
                ReplicaGroupLogOptions(sp))
            {
                Election = sp.Find<ElectionTimerOptions>() ?? new ElectionTimerOptions(),
                ElectionClock = sp.Require<TimeProvider>(),
                QuorumReads = cluster.QuorumReadsEnabled && LeadsByElection(cluster),
            };
        }

        /// <summary>Tells whether the groups of this node are led by election: automatic failover on and at least three replicas per group.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <returns><see langword="true" /> when an election driver leads every group; otherwise the owner leads its group statically.</returns>
        private static bool LeadsByElection(TopologyOptions cluster) => cluster is { AutomaticFailoverEnabled: true, ReplicaCount: >= 3 };

        /// <summary>Returns the settings of the replica group logs: the idempotency window of the replication policy and the host clock.</summary>
        /// <param name="sp">The service provider.</param>
        /// <returns>The follower log settings.</returns>
        /// <remarks>The policy values are hashed into the topology fingerprint, so every member of a group keeps outcomes for the same window.</remarks>
        private static FollowerLogOptions ReplicaGroupLogOptions(IServiceProvider sp) => new()
        {
            IdempotencyCapacity = PolicyOptions.RfIdempotencyMaxInFlightRecords,
            IdempotencyRetention = TimeSpan.FromTicks(PolicyOptions.RfIdempotencyRetentionTicks),
            IdempotencyMinRetention = TimeSpan.FromTicks(PolicyOptions.RfIdempotencyMinRetentionTicks),
            IdempotencyMaxOutcomeBytes = PolicyOptions.RfIdempotencyMaxOutcomeBytes,
            TimeProvider = sp.Require<TimeProvider>(),

            // The shutdown budget runs on the system clock, so a host clock that never advances cannot park a dispose.
            ShutdownTimeProvider = TimeProvider.System,
        };
    }

    private static class PersistenceOptionsResolver
    {
        internal static PersistenceOptions Resolve(TopologyOptions cluster, PersistenceOptions source, bool replicated)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(source);

            var dataDir = string.IsNullOrWhiteSpace(source.DataDir) ? DefaultDataDirectory.Resolve(cluster.ClusterId, cluster.NodeId) : source.DataDir;
            return source with { DataDir = dataDir, ValueLimitBytes = EntryLimits.ValueBytes(replicated) };
        }
    }
}
