using System;
using System.Diagnostics.Metrics;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class RuntimeServiceRegistration
{
    extension(NodeServices services)
    {
        internal NodeServices AddSquirixRuntimeServices(bool metricsEnabled)
        {
            _ = services.AddRequestContextServices();
            _ = services.Own<IBackpressureClientIdResolver>(static sp => sp.Require<AdmissionOptions>().HasPerClientLimits
                ? new BinaryClientIdResolver(sp.Require<BinaryRequestAccessor>())
                : SharedClientIdResolver.Instance);
            _ = services.Own<IBackpressureGate>(static sp => new AdmissionGate(
                sp.Require<AdmissionOptions>(),
                sp.Require<BackpressureMetrics>(),
                sp.Require<TimeProvider>()));
            _ = services.Own<IMemoryPressureStateEvaluator>(static sp => new StateEvaluator(sp.Require<PressureOptions>()));
            _ = services.Own(static sp => new MemoryUsageAccounting(sp.Require<ILocalCacheStats>()));
            _ = services.Own<IMemoryUsageAccounting>(static sp => sp.Require<MemoryUsageAccounting>());
            _ = services.AddObservabilityServices();
            _ = services.Own(static sp => new PressureGate(
                sp.Require<IMemoryPressureStateEvaluator>(),
                sp.Require<IMemoryUsageAccounting>(),
                sp.Require<TopologyOptions>().NodeId,
                sp.Find<Meter>()));
            _ = services.Own<ICriticalMemoryPressureReader>(static sp => new CriticalMemoryPressureReader(
                sp.Require<IMemoryPressureStateEvaluator>(),
                sp.Require<IMemoryUsageAccounting>()));
            _ = services.Own<ICacheEntrySizeEstimator<object?>>(static _ => new ObjectCacheEntrySizeEstimator());
            _ = services.AddLocalCacheServices();

            if (metricsEnabled)
                _ = services.AddMetricsHostedServices();
            _ = services.Own<ILocalCacheRecovery<object?>>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ILocalCacheSnapshotReader<object?>>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ISnapshotEntryCapture>(static sp => new LocalCacheSnapshotCapture<object?>(
                sp.Require<ILocalCacheSnapshotReader<object?>>(),
                sp.Require<CacheExpiryAuthority>()));

            _ = services.AddRequestRoutingServices();

            return services;
        }

        private NodeServices AddRequestContextServices()
        {
            _ = services.Own(static _ => new RemoteInvocationContextAccessor());
            _ = services.Own<IRemoteInvocationScopeFactory>(static sp => sp.Require<RemoteInvocationContextAccessor>());
            _ = services.Own<IRemoteInvocationState>(static sp => sp.Require<RemoteInvocationContextAccessor>());
            _ = services.Own(static _ => new BinaryRequestAccessor());
            _ = services.Own<IRoutedKeyContext>(static sp => sp.Require<BinaryRequestAccessor>());
            return services;
        }

        private NodeServices AddRequestRoutingServices()
        {
            _ = services.Own(static sp => CreateOwnerRouter(sp));
            _ = services.Own(static sp => new OwnerRpcForwarder(
                sp.Require<IServerClientPool>(),
                sp.Require<IBackpressureGate>(),
                sp.Require<IBackpressureClientIdResolver>(),
                sp.Require<RingAgreement>(),
                sp.Require<FeatureState>().NetworkReplicationEnabled));
            _ = services.Own(static sp => new RoutedCacheOperations<object?>(
                sp.Require<IInboundEndpointCacheOperations<object?>>(),
                sp.Require<OwnerRouter>(),
                sp.Require<OwnerRpcForwarder>(),
                sp.Require<FeatureState>().NetworkReplicationEnabled,
                sp.Require<TimeProvider>(),
                sp.Require<IDistributedCacheAccess>(),
                sp.Find<IJournalCoordinator>()));
            _ = services.Own(static sp => sp.Require<IInboundEndpointCacheOperations<object?>>().ForCache(ServerCacheNames.DefaultNamespace));

            return services;
        }

        private NodeServices AddMetricsHostedServices()
        {
            _ = services.RunService(static sp => new ItemsGaugeReporterService(sp.Require<ILocalCacheStats>(), sp.Require<Meter>()));
            _ = services.Own(static sp => new MemoryPressureMetrics(sp.Require<Meter>()));
            _ = services.RunService(static sp => new MemoryPressureMetricsService(
                sp.Require<TopologyOptions>(),
                sp.Require<IMemoryUsageAccounting>(),
                sp.Require<IMemoryPressureStateEvaluator>(),
                sp.Require<MemoryPressureMetrics>()));
            return services;
        }

        private NodeServices AddLocalCacheServices()
        {
            // Default server clock. Tests may register a fake TimeProvider that overrides this, so cache
            // expiration can be advanced deterministically instead of relying on real-time delays.
            _ = services.Use(TimeProvider.System);

            // Who decides expiry, read by the cache, its snapshots and recovery alike. FeatureState is the single
            // source of truth: on network-replication-activated hosts only committed records remove an entry, so the leader decides expiry;
            // RF=1 and foundation-only hosts keep the local clock.
            _ = services.Own(static sp => sp.Require<FeatureState>().NetworkReplicationEnabled ? CacheExpiryAuthority.CommittedRecords : CacheExpiryAuthority.LocalClock);
            _ = services.Own(static sp => new PhysicalCache<object?>(
                sp.Require<TimeProvider>(),
                sp.Require<ICacheEntrySizeEstimator<object?>>(),
                new EvictionOptions { Policy = EvictionPolicyType.Lru },
                sp.Require<CacheExpiryAuthority>()));
            _ = services.Own<ILocalCache<object?>>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ILocalCacheReadOperations<object?>>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ILocalCacheMutationOperations<object?>>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ILocalCacheStats>(static sp => sp.Require<PhysicalCache<object?>>());
            _ = services.Own<ILocalCacheRawReader<object?>>(static sp => sp.Require<PhysicalCache<object?>>().RawReader);
            _ = services.Own<ILocalCacheExpiredKeys>(static sp => sp.Require<PhysicalCache<object?>>().ExpiredKeys);
            return services;
        }

        private NodeServices AddObservabilityServices()
        {
            // The per-host Meter instance is owned by the host composition, which registers it through the factory
            // overload (so the node disposes it on shutdown) before AddSquirixRuntimeServices runs. This
            // method only registers the metrics types against that shared meter.
            _ = services.Own(static sp => BackpressureMetrics.Create(sp.Find<Meter>()));
            _ = services.Own(static sp => new ReplicationMetrics(sp.Find<Meter>()));
            _ = services.Own(static sp => new ReplicaCatchUpMetrics(sp.Find<Meter>()));
            _ = services.Own(static sp => new ServerCallPolicyMetrics(sp.Find<Meter>()));
            _ = services.Own(static sp => new ServerClientPoolMetrics(sp.Find<Meter>()));
            _ = services.Own(static sp => new ServerRpcTimeoutMetrics(sp.Find<Meter>()));
            _ = services.Own(static sp => new ServerCallPolicyInstrumentation(
                sp.Require<ServerCallPolicyMetrics>(),
                sp.Require<ServerRpcTimeoutMetrics>()));
            return services;
        }
    }

    /// <summary>Creates the router of single-key RPCs over the leader table of the node.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The router; it waits for a leader at most as long as the election options allow.</returns>
    private static OwnerRouter CreateOwnerRouter(IServiceProvider sp) => new(
        sp.Require<INodeOwnershipResolver>(),
        sp.Require<IRemoteInvocationState>(),
        sp.Require<RingAgreement>(),
        sp.Require<IGroupLeaderTable>(),
        (sp.Find<ElectionTimerOptions>() ?? new ElectionTimerOptions()).LeaderWaitTimeout,
        sp.Require<TimeProvider>(),
        sp.Require<IReplicaGroupLocator>())
    {
        KeyContext = sp.Require<IRoutedKeyContext>(),
    };

    /// <summary>DI-backed accessor for <see cref="RemoteInvocationContext" /> async-local state.</summary>
    [Immutable]
    private sealed class RemoteInvocationContextAccessor : IRemoteInvocationScopeFactory, IRemoteInvocationState
    {
        /// <inheritdoc />
        public bool IsInternalOwnerInvocation => RemoteInvocationContext.IsInternalOwnerInvocation;

        /// <inheritdoc />
        public RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation) => RemoteInvocationContext.EnterRemoteInvocation(isInternalOwnerInvocation);
    }
}
