using System;
using System.Diagnostics.Metrics;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Logging;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Hosting;

internal static class CachePipelineRegistration
{
    internal static NodeServices AddSquirixCachePipeline(this NodeServices services, bool persistenceEnabled = false, bool metricsEnabled = false)
    {
        _ = services.Own(static sp => new ClientCache<object?>(
            sp.Require<ILocalCacheReadOperations<object?>>(),
            sp.Require<ILocalCacheMutationOperations<object?>>()));

        AddOwnershipGuardLayer(services, persistenceEnabled);
        AddCacheDecoratorChain(services, metricsEnabled);
        _ = services.Own<ILogicalNamespacedCache<object?>>(static sp => sp.Require<TracingCacheDecorator<object?>>());
        _ = services.Own<IDistributedCacheAccess>(static sp => new NodeDistributedCacheAccess(
            ResolveLocalCache(sp),
            sp.Require<PhysicalCache<object?>>(),
            sp.Find<JournalLoggingCacheDecorator<object?>>(),
            sp.Require<IBackpressureGate>(),
            sp.Require<IBackpressureClientIdResolver>()));

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

    /// <summary>Creates the memory admission decorator over the owner-local cache.</summary>
    /// <param name="local">The owner-local cache; a <see cref="ReplicatedCache" /> on replication-activated hosts.</param>
    /// <param name="gate">The memory pressure gate.</param>
    /// <param name="estimator">The entry size estimator.</param>
    /// <param name="hasRecordedOutcome">The recorded-outcome lookup, or <see langword="null" /> on single-copy hosts.</param>
    /// <returns>The decorator; on replication-activated hosts it also refuses growing writes of a group while a member reports critical memory pressure.</returns>
    /// <remarks>
    /// Admission reads around a replicated write must never start an expiry commit: the write decides the expiry itself.
    /// An answer it gives without a log record comes from a fenced read, and never reports absent a key whose expired entry has no
    /// committed tombstone yet.
    /// </remarks>
    internal static MemoryAdmissionCacheDecorator<object?> CreateAdmissionDecorator(
        ILogicalNamespacedCache<object?> local,
        PressureGate gate,
        ICacheEntrySizeEstimator<object?> estimator,
        Func<string, string, string, bool>? hasRecordedOutcome)
    {
        var replicated = local as ReplicatedCache;
        return new MemoryAdmissionCacheDecorator<object?>(
            local,
            gate,
            estimator,
            hasRecordedOutcome,
            replicated == null ? null : replicated.PeekEntryAsync,
            replicated == null ? null : replicated.PeekFencedEntryAsync,
            replicated == null ? null : replicated.HasMemberPressure);
    }

    /// <summary>Resolves the owner-local cache: replicated commits on activated hosts, direct pipeline otherwise.</summary>
    /// <param name="sp">Service provider.</param>
    /// <returns>The local cache pipeline.</returns>
    private static ILogicalNamespacedCache<object?> ResolveLocalCache(IServiceProvider sp)
    {
        var inner = sp.Require<OwnerLocalCache>().Value;

        // FeatureState is the single source of truth: only network-replication-activated hosts commit.
        // RF=1 and foundation-only hosts keep the direct single-copy path untouched.
        if (!sp.Require<FeatureState>().NetworkReplicationEnabled)
            return inner;

        // Reads fence on the elected leader only under quorum reads, as the registry tells its elected leaders; a group led statically
        // has no read index to confirm.
        return new ReplicatedCache(inner, sp.Require<ReplicaGroupCommitters>(), sp.Require<ReplicaGroupRegistry>().QuorumReads);
    }

    /// <summary>
    /// Outermost decorator runs first: Tracing → DomainError → Validation → OwnershipGuard → Backpressure → Metrics → Memory.
    /// The ownership guard sits above admission and everything that commits, journals or takes key gates, so a remote key is refused before any of them.
    /// Validation stays outside Backpressure so invalid requests are rejected before taking an admission slot.
    /// </summary>
    /// <param name="services">Service collection receiving the decorator chain registrations.</param>
    /// <param name="metricsEnabled">Whether to wrap the cache with operation metrics.</param>
    private static void AddCacheDecoratorChain(NodeServices services, bool metricsEnabled)
    {
        _ = services.Own(static sp => CreateAdmissionDecorator(
            ResolveLocalCache(sp),
            sp.Require<PressureGate>(),
            sp.Require<ICacheEntrySizeEstimator<object?>>(),
            HasRecordedOutcome(sp)));
        if (metricsEnabled)
        {
            _ = services.Own(static sp => new CacheMetrics(sp.Require<Meter>()));
            _ = services.Own(static sp => new MetricsCacheDecorator<object?>(
                sp.Require<MemoryAdmissionCacheDecorator<object?>>(),
                sp.Require<CacheMetrics>()));
        }

        _ = services.Own(static sp => new BackpressureCacheDecorator<object?>(
            sp.Find<MetricsCacheDecorator<object?>>() is { } metrics
                ? metrics
                : sp.Require<MemoryAdmissionCacheDecorator<object?>>(),
            sp.Require<IBackpressureGate>(),
            sp.Require<IBackpressureClientIdResolver>()));
        _ = services.Own(static sp =>
        {
            // Only the table of the election state names elected leaders; the static table keeps the refusal trailers of static ownership.
            var leaders = sp.Require<IGroupLeaderTable>();
            return new OwnershipGuardCacheDecorator<object?>(
                sp.Require<TopologyOptions>().NodeId,
                sp.Require<INodeLocator>(),
                leaders,
                leaders is not StaticLeaderTable,
                sp.Require<BackpressureCacheDecorator<object?>>(),
                sp.Find<IRoutedKeyContext>());
        });
        _ = services.Own(static sp => new ValidationCacheDecorator<object?>(
            sp.Require<OwnershipGuardCacheDecorator<object?>>(),
            EntryLimits.ValueBytes(sp.Require<FeatureState>().NetworkReplicationEnabled),
            sp.Find<IRoutedKeyContext>()));
        _ = services.Own(static sp => new DomainErrorMappingCacheDecorator<object?>(sp.Require<ValidationCacheDecorator<object?>>()));
        _ = services.Own(static sp => new TracingCacheDecorator<object?>(
            sp.Require<DomainErrorMappingCacheDecorator<object?>>(),
            sp.Require<TopologyOptions>().NodeId));
    }

    private static void AddOwnershipGuardLayer(NodeServices services, bool persistenceEnabled)
    {
        if (persistenceEnabled)
        {
            _ = services.Own(static sp => new DurableMutationExecutor(sp.Require<IJournalCoordinator>(), sp.Require<IServerLoggerFactory>().CreateLogger<DurableMutationExecutor>()));
            _ = services.Own(static sp => new JournalLoggingCacheDecorator<object?>(
                sp.Require<ClientCache<object?>>(),
                sp.Require<IJournalCoordinator>(),
                sp.Require<DurableMutationExecutor>(),
                sp.Require<TimeProvider>(),
                sp.Require<ILocalCacheRawReader<object?>>()));
            _ = services.Own(static sp => new JournalPayloadPrepareCacheDecorator<object?>(sp.Require<JournalLoggingCacheDecorator<object?>>()));
            _ = services.Own(static sp => new OwnerLocalCache(sp.Require<JournalPayloadPrepareCacheDecorator<object?>>()));
            return;
        }

        _ = services.Own(static sp => new OwnerLocalCache(sp.Require<ClientCache<object?>>()));
    }

    /// <summary>Returns how memory admission asks whether the replica group of a key recorded an operation's outcome; none on single-copy hosts.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The lookup, or <see langword="null" /> when mutations do not commit through a replica group.</returns>
    private static Func<string, string, string, bool>? HasRecordedOutcome(IServiceProvider sp) =>
        sp.Require<FeatureState>().NetworkReplicationEnabled
            ? RecordedOutcomeLookup(sp.Require<ReplicaGroupRegistry>(), sp.Require<INodeLocator>())
            : null;
}
