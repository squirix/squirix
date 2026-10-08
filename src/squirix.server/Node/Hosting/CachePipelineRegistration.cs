using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
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
    /// <summary>Service key of the owner-local cache chain below replication: the target of replicated applies and of direct local calls.</summary>
    internal const string LocalChainKey = "local-chain";

    internal static IServiceCollection AddSquirixCachePipeline(this IServiceCollection services, ExtensionOptions? extensions = null, bool persistenceEnabled = false)
    {
        _ = services.AddSingleton(static sp => new ClientCache<object?>(
            sp.GetRequiredService<ILocalCacheReadOperations<object?>>(),
            sp.GetRequiredService<ILocalCacheMutationOperations<object?>>()));

        AddOwnershipGuardLayer(services, persistenceEnabled);
        AddCacheDecoratorChain(services);
        AddLogicalNamespacedCache(services, extensions);

        return services;
    }

    /// <summary>Creates the lookup that asks the replica group owning a key whether it recorded an operation's outcome.</summary>
    /// <param name="registry">The groups this node serves.</param>
    /// <param name="owners">The ring owner of a key, which names its replica group.</param>
    /// <returns>The lookup by cache name, key and operation id; a group this node does not serve records nothing.</returns>
    /// <remarks>
    /// A key that passed the ownership guard belongs to a group this node leads. Without elections that is always the group this node
    /// owns, so the lookup asks the same group as before; with elections each led group answers for its own keys.
    /// </remarks>
    internal static Func<string, string, string, bool> RecordedOutcomeLookup(ReplicaGroupRegistry registry, INodeLocator owners)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(owners);
        return (cacheName, key, operationId) => registry.HasRecordedOutcome(owners.GetOwner(cacheName, key), cacheName, operationId);
    }

    /// <summary>
    /// Outermost decorator runs first: Tracing → DomainError → Validation → OwnershipGuard → Backpressure → Metrics → Memory.
    /// The ownership guard sits above admission and everything that commits, journals or takes key gates, so a remote key is refused before any of them.
    /// Validation stays outside Backpressure so invalid requests are rejected before taking an admission slot.
    /// </summary>
    /// <param name="services">Service collection receiving the decorator chain registrations.</param>
    private static void AddCacheDecoratorChain(IServiceCollection services)
    {
        _ = services.AddSingleton(static sp =>
        {
            var local = ResolveLocalCache(sp);

            // Admission reads around a replicated write must never start an expiry commit: the write decides the expiry itself.
            return new MemoryAdmissionCacheDecorator<object?>(
                local,
                sp.GetRequiredService<IMemoryPressureGate>(),
                sp.GetRequiredService<ICacheEntrySizeEstimator<object?>>(),
                sp.GetRequiredService<IMemoryUsageAccounting>(),
                HasRecordedOutcome(sp),
                local is ReplicatedCache replicated ? replicated.PeekEntryAsync : null);
        });
        _ = services.AddSingleton(static sp => new MetricsCacheDecorator<object?>(
            sp.GetRequiredService<MemoryAdmissionCacheDecorator<object?>>(),
            sp.GetRequiredService<CacheMetrics>()));
        _ = services.AddSingleton(static sp => new BackpressureCacheDecorator<object?>(
            sp.GetRequiredService<MetricsCacheDecorator<object?>>(),
            sp.GetRequiredService<IBackpressureGate>(),
            sp.GetRequiredService<IBackpressureClientIdResolver>()));
        _ = services.AddSingleton(static sp =>
        {
            // Only the table of the election state names elected leaders; the static table keeps the refusal trailers of static ownership.
            var leaders = sp.GetRequiredService<IGroupLeaderTable>();
            return new OwnershipGuardCacheDecorator<object?>(
                sp.GetRequiredService<TopologyOptions>().NodeId,
                sp.GetRequiredService<INodeLocator>(),
                leaders,
                leaders is not StaticLeaderTable,
                sp.GetRequiredService<BackpressureCacheDecorator<object?>>());
        });
        _ = services.AddSingleton(static sp => new ValidationCacheDecorator<object?>(sp.GetRequiredService<OwnershipGuardCacheDecorator<object?>>()));
        _ = services.AddSingleton(static sp => new DomainErrorMappingCacheDecorator<object?>(sp.GetRequiredService<ValidationCacheDecorator<object?>>()));
        _ = services.AddSingleton(static sp => new TracingCacheDecorator<object?>(
            sp.GetRequiredService<DomainErrorMappingCacheDecorator<object?>>(),
            sp.GetRequiredService<TopologyOptions>().NodeId));
        services.TryAddSingleton<ISquirixServerEntryCachePipeline<object?>>(static sp =>
            new BasicExtensionCachePipelineAdapter<object?>(sp.GetRequiredService<TracingCacheDecorator<object?>>()));
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
            _ = services.AddSingleton(static sp => new DurableMutationExecutor(sp.GetRequiredService<IJournalCoordinator>(), sp.GetRequiredService<ILogger<DurableMutationExecutor>>()));
            _ = services.AddSingleton(static sp => new JournalLoggingCacheDecorator<object?>(
                sp.GetRequiredService<ClientCache<object?>>(),
                sp.GetRequiredService<IJournalCoordinator>(),
                sp.GetRequiredService<DurableMutationExecutor>(),
                sp.GetService<TimeProvider>(),
                sp.GetRequiredService<ILocalCacheRawReader<object?>>()));
            _ = services.AddSingleton(static sp => new JournalPayloadPrepareCacheDecorator<object?>(sp.GetRequiredService<JournalLoggingCacheDecorator<object?>>()));
            _ = services.AddKeyedSingleton<ILogicalNamespacedCache<object?>>(LocalChainKey, static (sp, _) => sp.GetRequiredService<JournalPayloadPrepareCacheDecorator<object?>>());
            return;
        }

        _ = services.AddSingleton(static sp => new OwnerPutPayloadGuardDecorator<object?>(sp.GetRequiredService<ClientCache<object?>>()));
        _ = services.AddKeyedSingleton<ILogicalNamespacedCache<object?>>(LocalChainKey, static (sp, _) => sp.GetRequiredService<OwnerPutPayloadGuardDecorator<object?>>());
    }

    /// <summary>Returns how memory admission asks whether the replica group of a key recorded an operation's outcome; none on single-copy hosts.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The lookup, or <see langword="null" /> when mutations do not commit through a replica group.</returns>
    private static Func<string, string, string, bool>? HasRecordedOutcome(IServiceProvider sp) =>
        sp.GetRequiredService<FeatureState>().NetworkReplicationEnabled
            ? RecordedOutcomeLookup(sp.GetRequiredService<ReplicaGroupRegistry>(), sp.GetRequiredService<INodeLocator>())
            : null;

    /// <summary>Resolves the owner-local cache: replicated commits on activated hosts, direct pipeline otherwise.</summary>
    /// <param name="sp">Service provider.</param>
    /// <returns>The local cache pipeline.</returns>
    private static ILogicalNamespacedCache<object?> ResolveLocalCache(IServiceProvider sp)
    {
        var inner = sp.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(LocalChainKey);

        // FeatureState is the single source of truth: only network-replication-activated hosts commit.
        // RF=1 and foundation-only hosts keep the direct single-copy path untouched.
        if (!sp.GetRequiredService<FeatureState>().NetworkReplicationEnabled)
            return inner;

        // Reads fence on the elected leader only under quorum reads, as the registry tells its elected leaders; a group led statically
        // has no read index to confirm.
        return new ReplicatedCache(inner, sp.GetRequiredService<ReplicaGroupCommitters>(), sp.GetRequiredService<ReplicaGroupRegistry>().QuorumReads);
    }
}
