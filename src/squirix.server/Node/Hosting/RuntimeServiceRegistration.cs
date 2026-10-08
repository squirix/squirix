using System;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class RuntimeServiceRegistration
{
    extension(IServiceCollection services)
    {
        internal IServiceCollection AddSquirixRuntimeServices()
        {
            _ = services.AddSingleton<RemoteInvocationContextAccessor>();
            _ = services.AddSingleton<IRemoteInvocationScopeFactory>(static sp => sp.GetRequiredService<RemoteInvocationContextAccessor>());
            _ = services.AddSingleton<IRemoteInvocationState>(static sp => sp.GetRequiredService<RemoteInvocationContextAccessor>());
            _ = services.AddHttpContextAccessor();
            _ = services.AddSingleton<IBackpressureClientIdResolver>(static sp => sp.GetRequiredService<AdmissionOptions>().HasPerClientLimits
                ? new HttpContextClientIdResolver(sp.GetRequiredService<IHttpContextAccessor>())
                : SharedClientIdResolver.Instance);
            _ = services.AddSingleton<IBackpressureGate>(static sp => new AdmissionGate(sp.GetRequiredService<AdmissionOptions>(), sp.GetRequiredService<BackpressureMetrics>(), sp.GetService<TimeProvider>()));
            _ = services.AddSingleton<IMemoryPressureStateEvaluator>(static sp => new StateEvaluator(sp.GetRequiredService<IOptions<PressureOptions>>()));
            _ = services.AddSingleton<MemoryUsageAccounting>();
            _ = services.AddSingleton<IMemoryUsageAccounting>(static sp => sp.GetRequiredService<MemoryUsageAccounting>());
            _ = services.AddObservabilityServices();
            _ = services.AddSingleton<IMemoryPressureGate>(static sp => new PressureGate(
                sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
                sp.GetRequiredService<IMemoryUsageAccounting>(),
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton<IBackgroundSnapshotMemoryThrottle>(static sp => new BackgroundSnapshotMemoryThrottle(
                sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
                sp.GetRequiredService<IMemoryUsageAccounting>()));
            _ = services.AddSingleton<ICacheEntrySizeEstimator<object?>>(static _ => new ObjectCacheEntrySizeEstimator());
            _ = services.AddLocalCacheServices();

            _ = services.AddHostedServices();
            _ = services.AddSingleton<ILocalCacheRecovery<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ILocalCacheSnapshotReader<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ISnapshotEntryCapture>(static sp => new LocalCacheSnapshotCapture<object?>(
                sp.GetRequiredService<ILocalCacheSnapshotReader<object?>>(),
                sp.GetRequiredService<CacheExpiryAuthority>()));

            _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
            _ = services.AddSingleton<IGrpcCacheOperations<object?>, CacheOperations<object?>>();
            _ = services.AddSingleton(static sp => new OwnerRouter(
                sp.GetRequiredService<INodeOwnershipResolver>(),
                sp.GetRequiredService<IRemoteInvocationState>(),
                sp.GetRequiredService<RingAgreement>()));
            _ = services.AddSingleton(static sp => new OwnerRpcForwarder(
                sp.GetRequiredService<IServerClientPool>(),
                sp.GetRequiredService<IBackpressureGate>(),
                sp.GetRequiredService<IBackpressureClientIdResolver>(),
                sp.GetRequiredService<RingAgreement>()));
            _ = services.AddSingleton(static sp => new RpcMutationIdempotencyStore(
                sp.GetRequiredService<IdempotencyOptions>(),
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<IdempotencyMetrics>(),
                sp.GetService<TimeProvider>()));
            _ = services.AddSingleton<IIdempotencySnapshotExporter>(static sp => sp.GetRequiredService<RpcMutationIdempotencyStore>());
            _ = services.AddSingleton(static sp =>
            {
                var store = sp.GetRequiredService<RpcMutationIdempotencyStore>();
                var journal = sp.GetService<IJournalCoordinator>();
                var logger = sp.GetRequiredService<ILogger<RpcMutationIdempotencyCoordinator>>();
                return journal != null ? new RpcMutationIdempotencyCoordinator(store, journal, logger) : new RpcMutationIdempotencyCoordinator(store, logger);
            });
            _ = services.AddSingleton<IRpcMutationIdempotencyCoordinator>(static sp => sp.GetRequiredService<RpcMutationIdempotencyCoordinator>());
            _ = services.AddSingleton(static sp => sp.GetRequiredService<IInboundEndpointCacheOperations<object?>>().ForCache(ServerCacheNames.DefaultNamespace));

            return services;
        }

        private IServiceCollection AddHostedServices()
        {
            _ = services.AddHostedService(static sp => new ItemsGaugeReporterService(sp.GetRequiredService<ILocalCacheStats>(), sp.GetRequiredService<Meter>()));
            _ = services.AddHostedService<MemoryPressureMetricsService>();
            _ = services.AddHostedService<IdempotencyMetricsService>();
            _ = services.AddHostedService<IdempotencyStoreSweepService>();
            return services;
        }

        private IServiceCollection AddLocalCacheServices()
        {
            // Default server clock. Tests may register a fake TimeProvider that overrides this, so cache
            // expiration can be advanced deterministically instead of relying on real-time delays.
            _ = services.AddSingleton(TimeProvider.System);

            // Who decides expiry, read by the cache, its snapshots, recovery and journal compaction alike. FeatureState is the single
            // source of truth: on network-replication-activated hosts only committed records remove an entry, so the leader decides expiry;
            // RF=1 and foundation-only hosts keep the local clock. AddSingleton<T>(T) is constrained to class, so the descriptor boxes the enum.
            services.Add(new ServiceDescriptor(
                typeof(CacheExpiryAuthority),
                static sp => sp.GetRequiredService<FeatureState>().NetworkReplicationEnabled ? CacheExpiryAuthority.CommittedRecords : CacheExpiryAuthority.LocalClock,
                ServiceLifetime.Singleton));
            _ = services.AddSingleton(static sp => new PhysicalCache<object?>(
                sp.GetService<TimeProvider>(),
                new EvictionOptions { Policy = EvictionPolicyType.Lru },
                sp.GetRequiredService<CacheExpiryAuthority>()));
            _ = services.AddSingleton<ILocalCache<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ILocalCacheReadOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ILocalCacheMutationOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ILocalCacheStats>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
            _ = services.AddSingleton<ILocalCacheRawReader<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>().RawReader);
            return services;
        }

        private IServiceCollection AddObservabilityServices()
        {
            // The per-host Meter instance is owned by the host composition, which registers it through the factory
            // overload (so the DI container disposes it on shutdown) before AddSquirixRuntimeServices runs. This
            // method only registers the metrics types against that shared meter.
            _ = services.AddSingleton(static sp => new BackpressureMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new CacheMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new CompactionMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new IdempotencyMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new MemoryPressureMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ReplicationMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ReplicaCatchUpMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ServerCallPolicyMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ServerClientPoolMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ServerRpcTimeoutMetrics(sp.GetRequiredService<Meter>()));
            _ = services.AddSingleton(static sp => new ServerCallPolicyInstrumentation(
                sp.GetRequiredService<ServerCallPolicyMetrics>(),
                sp.GetRequiredService<ServerRpcTimeoutMetrics>()));
            return services;
        }
    }

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
