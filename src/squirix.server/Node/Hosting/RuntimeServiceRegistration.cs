using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class RuntimeServiceRegistration
{
    internal static IServiceCollection AddSquirixRuntimeServices(this IServiceCollection services)
    {
        _ = services.AddSingleton<RemoteInvocationContextAccessor>();
        _ = services.AddSingleton<IRemoteInvocationScopeFactory>(static sp => sp.GetRequiredService<RemoteInvocationContextAccessor>());
        _ = services.AddSingleton<IRemoteInvocationState>(static sp => sp.GetRequiredService<RemoteInvocationContextAccessor>());
        _ = services.AddSingleton<IServerSerializer>(static _ => new ServerMetricsSerializer(new ServerJsonSerializer()));
        _ = services.AddSingleton<IBackpressureGate>(static sp => new AdmissionGate(sp.GetRequiredService<AdmissionOptions>()));
        _ = services.AddSingleton<IMemoryPressureStateEvaluator>(static sp => new StateEvaluator(sp.GetRequiredService<IOptions<PressureOptions>>()));
        _ = services.AddSingleton<MemoryUsageAccounting>();
        _ = services.AddSingleton<IMemoryUsageAccounting>(static sp => sp.GetRequiredService<MemoryUsageAccounting>());
        _ = services.AddSingleton<IMemoryPressureGate>(static sp => new PressureGate(
            sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
            sp.GetRequiredService<IMemoryUsageAccounting>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddSingleton<IBackgroundSnapshotMemoryThrottle>(static sp => new BackgroundSnapshotMemoryThrottle(
            sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
            sp.GetRequiredService<IMemoryUsageAccounting>()));
        _ = services.AddSingleton<ICacheEntrySizeEstimator<object?>>(static _ => new ObjectCacheEntrySizeEstimator());

        _ = services.AddSingleton(static _ => new PhysicalCache<object?>(null, new EvictionOptions { Policy = EvictionPolicyType.Lru }));
        _ = services.AddSingleton<ILocalCache<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheReadOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheMutationOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheStats>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddHostedService(static sp => new ItemsGaugeReporterService(sp.GetRequiredService<ILocalCacheStats>()));
        _ = services.AddHostedService<MemoryPressureMetricsService>();
        _ = services.AddHostedService<IdempotencyMetricsService>();
        _ = services.AddHostedService<IdempotencyStoreSweepService>();
        _ = services.AddSingleton<ILocalCacheRecovery<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheSnapshotReader<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ISnapshotEntryCapture>(static sp => new LocalCacheSnapshotCapture<object?>(sp.GetRequiredService<ILocalCacheSnapshotReader<object?>>()));

        _ = services.AddSingleton<ICacheRuntime, CacheRuntime>();
        _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
        _ = services.AddSingleton<IGrpcCacheOperations<object?>, CacheOperations<object?>>();
        _ = services.AddSingleton(static sp => new RpcMutationIdempotencyStore(sp.GetRequiredService<IdempotencyOptions>(), sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddSingleton<IIdempotencySnapshotExporter>(static sp => sp.GetRequiredService<RpcMutationIdempotencyStore>());
        _ = services.AddSingleton(static sp =>
        {
            var store = sp.GetRequiredService<RpcMutationIdempotencyStore>();
            var journal = sp.GetService<IJournalCoordinator>();
            return journal is not null ? new RpcMutationIdempotencyCoordinator(store, journal) : new RpcMutationIdempotencyCoordinator(store);
        });
        _ = services.AddSingleton<IRpcMutationIdempotencyCoordinator>(static sp => sp.GetRequiredService<RpcMutationIdempotencyCoordinator>());
        _ = services.AddSingleton(static sp => sp.GetRequiredService<IInboundEndpointCacheOperations<object?>>().ForCache(ServerCacheNames.DefaultNamespace));

        return services;
    }

    /// <summary>DI-backed accessor for <see cref="RemoteInvocationContext" /> async-local state.</summary>
    private sealed class RemoteInvocationContextAccessor : IRemoteInvocationScopeFactory, IRemoteInvocationState
    {
        /// <inheritdoc />
        public bool IsInternalOwnerInvocation => RemoteInvocationContext.IsInternalOwnerInvocation;

        /// <inheritdoc />
        public RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation) => RemoteInvocationContext.EnterRemoteInvocation(isInternalOwnerInvocation);
    }
}
