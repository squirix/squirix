using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Hosting;

internal static class CachePipelineRegistration
{
    internal static IServiceCollection AddSquirixCachePipeline(this IServiceCollection services, ExtensionOptions? extensions = null, bool persistenceEnabled = false)
    {
        _ = services.AddOptions<CachePipelineDeadlineOptions>();
        _ = services.AddSingleton(static sp => new ClientCache<object?>(
            sp.GetRequiredService<ILocalCacheReadOperations<object?>>(),
            sp.GetRequiredService<ILocalCacheMutationOperations<object?>>()));

        AddOwnershipGuardLayer(services, persistenceEnabled);
        AddClusteredCacheSingleton(services);
        AddCacheDecoratorChain(services);
        AddLogicalNamespacedCache(services, extensions);

        return services;
    }

    /// <summary>
    /// Outermost decorator runs first: Tracing → DomainError → Validation → Backpressure → Deadline → Metrics → Memory.
    /// Backpressure stays outside Deadline on purpose: admission shaping (slowdown plus queue wait) must not
    /// consume the operation execution budget, otherwise a saturated node times out work that never ran (#456).
    /// Validation stays outside Backpressure so invalid requests are rejected before taking an admission slot.
    /// </summary>
    /// <param name="services">Service collection receiving the decorator chain registrations.</param>
    private static void AddCacheDecoratorChain(IServiceCollection services)
    {
        _ = services.AddSingleton(static sp => new MemoryAdmissionCacheDecorator<object?>(
            sp.GetRequiredService<ClusteredCache<object?>>(),
            sp.GetRequiredService<IMemoryPressureGate>(),
            sp.GetRequiredService<ICacheEntrySizeEstimator<object?>>(),
            sp.GetRequiredService<IMemoryUsageAccounting>(),
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddSingleton(static sp => new MetricsCacheDecorator<object?>(
            sp.GetRequiredService<MemoryAdmissionCacheDecorator<object?>>(),
            sp.GetRequiredService<CacheMetrics>()));
        _ = services.AddSingleton(static sp => new DeadlineCacheDecorator<object?>(
            sp.GetRequiredService<MetricsCacheDecorator<object?>>(),
            sp.GetRequiredService<IOptions<CachePipelineDeadlineOptions>>()));
        _ = services.AddSingleton(static sp => new BackpressureCacheDecorator<object?>(
            sp.GetRequiredService<DeadlineCacheDecorator<object?>>(),
            sp.GetRequiredService<IBackpressureGate>(),
            sp.GetRequiredService<IBackpressureClientIdResolver>()));
        _ = services.AddSingleton(static sp => new ValidationCacheDecorator<object?>(
            sp.GetRequiredService<BackpressureCacheDecorator<object?>>(),
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddSingleton(static sp => new DomainErrorMappingCacheDecorator<object?>(sp.GetRequiredService<ValidationCacheDecorator<object?>>()));
        _ = services.AddSingleton(static sp => new TracingCacheDecorator<object?>(
            sp.GetRequiredService<DomainErrorMappingCacheDecorator<object?>>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        services.TryAddSingleton<ISquirixServerEntryCachePipeline<object?>>(static sp =>
            new BasicExtensionCachePipelineAdapter<object?>(sp.GetRequiredService<TracingCacheDecorator<object?>>()));
    }

    private static void AddClusteredCacheSingleton(IServiceCollection services)
    {
        _ = services.AddSingleton(static sp => new ClusteredCache<object?>(
            sp.GetRequiredService<TopologyOptions>().NodeId,
            ResolveLocalCache(sp),
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<IServerClientPool>()));
    }

    private static void AddLogicalNamespacedCache(IServiceCollection services, ExtensionOptions? extensions)
    {
        _ = services.AddSingleton<ILogicalNamespacedCache<object?>>(sp =>
        {
            var corePipeline = sp.GetRequiredService<TracingCacheDecorator<object?>>();
            var basicPipeline = new BasicExtensionCachePipelineAdapter<object?>(corePipeline);
            var decoratedPipeline = extensions?.DecorateCachePipeline?.Invoke(sp, basicPipeline);
            var isUndecorated = decoratedPipeline == null || ReferenceEquals(decoratedPipeline, basicPipeline);
            return isUndecorated ? corePipeline : new ExtensionCachePipelineAdapter<object?>(corePipeline, decoratedPipeline!);
        });
    }

    private static void AddOwnershipGuardLayer(IServiceCollection services, bool persistenceEnabled)
    {
        if (persistenceEnabled)
        {
            _ = services.AddSingleton(static sp => new DurableMutationExecutor(sp.GetRequiredService<IJournalCoordinator>()));
            _ = services.AddSingleton(static sp => new JournalLoggingCacheDecorator<object?>(
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<ClientCache<object?>>(),
                sp.GetRequiredService<IJournalCoordinator>(),
                sp.GetRequiredService<DurableMutationExecutor>()));
            _ = services.AddSingleton(static sp => new JournalPayloadPrepareCacheDecorator<object?>(
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<JournalLoggingCacheDecorator<object?>>()));
            _ = services.AddSingleton(static sp => new OwnershipGuardCacheDecorator<object?>(
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                sp.GetRequiredService<JournalPayloadPrepareCacheDecorator<object?>>()));
            return;
        }

        _ = services.AddSingleton(static sp => new OwnerPutPayloadGuardDecorator<object?>(
            sp.GetRequiredService<TopologyOptions>().NodeId,
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<ClientCache<object?>>()));
        _ = services.AddSingleton(static sp => new OwnershipGuardCacheDecorator<object?>(
            sp.GetRequiredService<TopologyOptions>().NodeId,
            sp.GetRequiredService<INodeLocator>(),
            sp.GetRequiredService<OwnerPutPayloadGuardDecorator<object?>>()));
    }

    /// <summary>Resolves the owner-local cache: replicated commits on activated hosts, direct pipeline otherwise.</summary>
    /// <param name="sp">Service provider.</param>
    /// <returns>The local cache pipeline.</returns>
    private static ILogicalNamespacedCache<object?> ResolveLocalCache(IServiceProvider sp)
    {
        var inner = sp.GetRequiredService<OwnershipGuardCacheDecorator<object?>>();

        // FeatureState is the single source of truth: only network-replication-activated hosts commit.
        // RF=1 and foundation-only hosts keep the direct single-copy path untouched.
        return sp.GetRequiredService<FeatureState>().NetworkReplicationEnabled ? new ReplicatedCache(inner, sp.GetRequiredService<ReplicaGroupCommitter>()) : inner;
    }
}
