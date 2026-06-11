using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Routing;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixCachePipelineRegistration
{
    public static IServiceCollection AddSquirixCachePipeline(this IServiceCollection services, SquirixServerExtensionOptions? extensions = null, bool persistenceEnabled = false)
    {
        _ = services.AddOptions<CachePipelineDeadlineOptions>();
        _ = services.AddSingleton<ClientCache<object?>>(static sp => new ClientCache<object?>(
            sp.GetRequiredService<ILocalCacheReadOperations<object?>>(),
            sp.GetRequiredService<ILocalCacheMutationOperations<object?>>()));

        if (persistenceEnabled)
        {
            _ = services.AddSingleton<DurableMutationExecutor>();
            _ = services.AddSingleton<JournalLoggingCacheDecorator<object?>>(static sp => new JournalLoggingCacheDecorator<object?>(
                sp.GetRequiredService<ClusterConfig>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<ClientCache<object?>>(),
                sp.GetRequiredService<IJournalCoordinator>(),
                sp.GetRequiredService<DurableMutationExecutor>()));
            _ = services.AddSingleton<OwnershipGuardCacheDecorator<object?>>(static sp => new OwnershipGuardCacheDecorator<object?>(
                sp.GetRequiredService<ClusterConfig>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<JournalLoggingCacheDecorator<object?>>()));
        }
        else
        {
            _ = services.AddSingleton<OwnershipGuardCacheDecorator<object?>>(static sp => new OwnershipGuardCacheDecorator<object?>(
                sp.GetRequiredService<ClusterConfig>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<ClientCache<object?>>()));
        }

        _ = services.AddClusteredCacheSingleton();
        _ = services.AddSingleton<MemoryAdmissionCacheDecorator<object?>>(static sp => new MemoryAdmissionCacheDecorator<object?>(
            sp.GetRequiredService<ClusteredCache<object?>>(),
            sp.GetRequiredService<IMemoryPressureGate>(),
            sp.GetRequiredService<ICacheEntrySizeEstimator<object?>>(),
            sp.GetRequiredService<IMemoryUsageAccounting>(),
            sp.GetRequiredService<ClusterConfig>().NodeId,
            sp.GetRequiredService<INodeLocator>()));
        _ = services.AddSingleton<MetricsCacheDecorator<object?>>(static sp => new MetricsCacheDecorator<object?>(sp.GetRequiredService<MemoryAdmissionCacheDecorator<object?>>()));
        _ = services.AddSingleton<BackpressureCacheDecorator<object?>>(static sp => new BackpressureCacheDecorator<object?>(
            sp.GetRequiredService<MetricsCacheDecorator<object?>>(),
            sp.GetRequiredService<IBackpressureGate>()));
        _ = services.AddSingleton<ValidationCacheDecorator<object?>>(static sp => new ValidationCacheDecorator<object?>(
            sp.GetRequiredService<BackpressureCacheDecorator<object?>>()));
        _ = services.AddSingleton<DeadlineCacheDecorator<object?>>(static sp => new DeadlineCacheDecorator<object?>(
            sp.GetRequiredService<ValidationCacheDecorator<object?>>(),
            sp.GetRequiredService<IOptions<CachePipelineDeadlineOptions>>()));
        _ = services.AddSingleton<DomainErrorMappingCacheDecorator<object?>>(static sp => new DomainErrorMappingCacheDecorator<object?>(
            sp.GetRequiredService<DeadlineCacheDecorator<object?>>()));
        _ = services.AddSingleton<TracingCacheDecorator<object?>>(static sp => new TracingCacheDecorator<object?>(
            sp.GetRequiredService<DomainErrorMappingCacheDecorator<object?>>(),
            sp.GetRequiredService<ClusterConfig>().NodeId));
        services.TryAddSingleton<ISquirixServerEntryCachePipeline<object?>>(static sp =>
            new BasicExtensionCachePipelineAdapter<object?>(sp.GetRequiredService<TracingCacheDecorator<object?>>()));
        _ = services.AddSingleton<ILogicalNamespacedCache<object?>>(sp =>
        {
            var corePipeline = sp.GetRequiredService<TracingCacheDecorator<object?>>();
            var basicPipeline = new BasicExtensionCachePipelineAdapter<object?>(corePipeline);
            var decoratedPipeline = extensions?.DecorateCachePipeline?.Invoke(sp, basicPipeline);
            return decoratedPipeline is null || ReferenceEquals(decoratedPipeline, basicPipeline)
                ? corePipeline
                : new ExtensionCachePipelineAdapter<object?>(corePipeline, decoratedPipeline);
        });

        return services;
    }
}
