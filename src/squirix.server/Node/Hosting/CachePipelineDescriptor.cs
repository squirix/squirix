using System;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Canonical pipeline ordering for the hosted object cache. This is a correctness contract: callers rely on
/// decorator ordering for validation, admission, journal durability, routing, and observability semantics.
/// </summary>
/// <remarks>
///     <para>
///     <see cref="AddSingletonRegistrationInnerToOuter" /> lists the same stack in <strong>source registration order</strong>
///     used by <see cref="SquirixCachePipelineRegistration.AddSquirixCachePipeline" />: inner layers are registered first.
///     The clustered layer invokes <c>AddClusteredCacheSingleton()</c> (extension on <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection" /> declared in
///     <c>ClusterRuntimeServiceRegistration</c>) from the hosting file;
///     the <c>AddSingleton&lt;ClusteredCache&lt;object?&gt;&gt;</c> factory lives in <c>Cluster/ClusterRuntimeServiceRegistration.cs</c>.
///     <see cref="CachePipelineLayer.MemoryAccounting" /> is omitted because it is not a separate DI singleton.
///     </para>
/// </remarks>
internal static class CachePipelineDescriptor
{
    /// <summary>
    /// Marker for the logical aggregate registration that resolves the outermost decorator instance.
    /// </summary>
    internal const string LogicalAggregateRegistrationMarker = "AddSingleton<ILogicalNamespacedCache<object?>>(sp =>";

    /// <summary>
    /// Gets <c>AddSingleton</c> registration order in <see cref="SquirixCachePipelineRegistration" /> (inner toward outer).
    /// Does not include <see cref="CachePipelineLayer.MemoryAccounting" />, which is created inside the Journal factory.
    /// </summary>
    internal static ReadOnlySpan<CachePipelineLayer> AddSingletonRegistrationInnerToOuter =>
    [
        CachePipelineLayer.ClientCache,
        CachePipelineLayer.JournalLogging,
        CachePipelineLayer.OwnershipGuard,
        CachePipelineLayer.Clustered,
        CachePipelineLayer.MemoryAdmission,
        CachePipelineLayer.Metrics,
        CachePipelineLayer.Backpressure,
        CachePipelineLayer.Validation,
        CachePipelineLayer.Deadline,
        CachePipelineLayer.DomainErrorMapping,
        CachePipelineLayer.Tracing,
    ];

    /// <summary>
    /// Returns a stable substring that marks the layer's primary <c>AddSingleton</c> registration for the object cache pipeline.
    /// </summary>
    /// <param name="layer">The pipeline layer.</param>
    /// <returns>A non-empty substring unique to that registration in the hosted pipeline sources (hosting and cluster registration files).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="layer" /> is <see cref="CachePipelineLayer.MemoryAccounting" />.</exception>
    internal static string AddSingletonRegistrationMarker(CachePipelineLayer layer) => layer switch
    {
        CachePipelineLayer.ClientCache => "AddSingleton<ClientCache<object?>>",
        CachePipelineLayer.JournalLogging => "AddSingleton<JournalLoggingCacheDecorator<object?>>",
        CachePipelineLayer.OwnershipGuard => "AddSingleton<OwnershipGuardCacheDecorator<object?>>",
        CachePipelineLayer.Clustered => "AddClusteredCacheSingleton()",
        CachePipelineLayer.MemoryAdmission => "AddSingleton<MemoryAdmissionCacheDecorator<object?>>",
        CachePipelineLayer.Metrics => "AddSingleton<MetricsCacheDecorator<object?>>",
        CachePipelineLayer.Backpressure => "AddSingleton<BackpressureCacheDecorator<object?>>",
        CachePipelineLayer.Validation => "AddSingleton<ValidationCacheDecorator<object?>>",
        CachePipelineLayer.Deadline => "AddSingleton<DeadlineCacheDecorator<object?>>",
        CachePipelineLayer.DomainErrorMapping => "AddSingleton<DomainErrorMappingCacheDecorator<object?>>",
        CachePipelineLayer.Tracing => "AddSingleton<TracingCacheDecorator<object?>>",
        CachePipelineLayer.MemoryAccounting => throw new ArgumentOutOfRangeException(nameof(layer), layer, "Memory accounting is not a separate AddSingleton registration."),
        _ => throw new ArgumentOutOfRangeException(nameof(layer), layer, "Unsupported pipeline layer."),
    };
}
