using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Node.Context;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Serialization;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixRuntimeServiceRegistration
{
    public static IServiceCollection AddSquirixRuntimeServices(this IServiceCollection services, CacheRuntimeOptions? runtimeOptions)
    {
        _ = services.AddSingleton<RemoteInvocationContextService>();
        _ = services.AddSingleton<IRemoteInvocationScopeFactory>(static sp => sp.GetRequiredService<RemoteInvocationContextService>());
        _ = services.AddSingleton<IRemoteInvocationState>(static sp => sp.GetRequiredService<RemoteInvocationContextService>());
        _ = services.AddSingleton(static sp => sp.GetRequiredService<IOptions<ClusterConfig>>().Value);
        _ = services.AddSingleton<ISquirixSerializer>(static _ => SerializationProvider.Instance);
        _ = services.AddSingleton(static sp => sp.GetRequiredService<IOptions<BackpressureOptions>>().Value);
        _ = services.AddSingleton<IBackpressureGate, BackpressureGate>();
        _ = services.AddSingleton(static sp => sp.GetRequiredService<IOptions<MemoryPressureOptions>>().Value);
        _ = services.AddSingleton<IMemoryPressureStateEvaluator, MemoryPressureStateEvaluator>();
        _ = services.AddSingleton<MemoryUsageAccounting>();
        _ = services.AddSingleton<IMemoryUsageAccounting>(static sp => sp.GetRequiredService<MemoryUsageAccounting>());
        _ = services.AddSingleton<IMemoryPressureGate>(static sp => new MemoryPressureGate(
            sp.GetRequiredService<IOptions<MemoryPressureOptions>>(),
            sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
            sp.GetRequiredService<IMemoryUsageAccounting>(),
            sp.GetRequiredService<ClusterConfig>().NodeId));
        _ = services.AddSingleton<ICacheEntrySizeEstimator<object?>>(static _ => new CacheEntrySizeEstimator<object?>());

        _ = services.AddSingleton(static _ => new PhysicalCache<object?>(null, new EvictionOptions { Policy = EvictionPolicyType.Lru }));
        _ = services.AddSingleton<ILocalCache<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheReadOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheMutationOperations<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheStats>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddHostedService(static sp => new ItemsGaugeReporterService(sp.GetRequiredService<ILocalCacheStats>()));
        _ = services.AddSingleton<ILocalCacheRecovery<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());
        _ = services.AddSingleton<ILocalCacheSnapshotReader<object?>>(static sp => sp.GetRequiredService<PhysicalCache<object?>>());

        _ = services.AddSingleton(runtimeOptions ?? new CacheRuntimeOptions());
        _ = services.AddSingleton<ICacheRuntime, CacheRuntime>();
        _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
        _ = services.AddSingleton<IGrpcCacheOperations<object?>, GrpcCacheOperations<object?>>();
        _ = services.AddSingleton<ICacheApi<object?>>(static sp => sp.GetRequiredService<IInboundEndpointCacheOperations<object?>>().ForCache(CacheNames.DefaultNamespace));

        return services;
    }
}
