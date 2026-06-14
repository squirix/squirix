using System;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Architecture;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Shared assertions for <see cref="SquirixCachePipelineRegistration" /> registration order vs <see cref="CachePipelineDescriptor" />.
/// </summary>
internal static class CachePipelineRegistrationOrderAssertions
{
    /// <summary>
    /// Asserts singleton layers are registered in <see cref="CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter" /> order.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    public static void AssertSingletonOrderMatchesDescriptor(string source)
    {
        AssertRegistrationCallOrder(source);

        var order = CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter;
        Assert.True(order.Length >= 2, "Descriptor must list at least two singleton layers.");

        foreach (var layer in order)
        {
            AssertMarkerPresent(source, layer);
        }

        for (var i = 0; i < order.Length - 1; i++)
        {
            AssertLayerRegisteredBefore(order[i], order[i + 1]);
        }
    }

    /// <summary>
    /// Asserts the logical aggregate registration runs after the outer decorator chain (tracing) is registered.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    public static void AssertTracingPrecedesLogicalAggregate(string source)
    {
        AssertRegistrationCallOrder(source);
        AssertMarkerPresent(source, CachePipelineLayer.Tracing);
        _ = IndexOfRequired(source, CachePipelineDescriptor.LogicalAggregateRegistrationMarker, "ILogicalNamespacedCache registration");
    }

    /// <summary>
    /// Loads the pipeline registration source file from the repository layout.
    /// </summary>
    /// <returns>File contents.</returns>
    public static string LoadPipelineRegistrationSource() =>
        ArchitectureRepositoryPaths.ReadSquirixLibrarySource(PathKit.Combine("Node", "Hosting", "SquirixCachePipelineRegistration.cs"));

    /// <summary>
    /// Returns the canonical registration sequence index for <paramref name="layer" />.
    /// </summary>
    /// <param name="layer">The pipeline layer.</param>
    /// <returns>Zero-based index in inner-to-outer registration order.</returns>
    internal static int GetRegistrationSequenceIndex(CachePipelineLayer layer)
    {
        var order = CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter;
        for (var i = 0; i < order.Length; i++)
        {
            if (order[i] == layer)
                return i;
        }

        throw new ArgumentOutOfRangeException(nameof(layer), layer, "Unsupported pipeline layer.");
    }

    internal static void AssertLayerRegisteredBefore(CachePipelineLayer inner, CachePipelineLayer outer)
    {
        Assert.True(
            GetRegistrationSequenceIndex(inner) < GetRegistrationSequenceIndex(outer),
            $"{inner} must register before {outer} in AddSquirixCachePipeline.");
    }

    internal static int IndexOfRequired(string source, CachePipelineLayer layer) =>
        IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(layer), layer.ToString());

    private static void AssertMarkerPresent(string source, CachePipelineLayer layer) =>
        IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(layer), layer.ToString());

    private static void AssertRegistrationCallOrder(string source)
    {
        var clientCache = IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(CachePipelineLayer.ClientCache), nameof(CachePipelineLayer.ClientCache));
        var ownershipLayer = IndexOfRequired(source, "AddOwnershipGuardLayer(", "AddOwnershipGuardLayer call");
        var clustered = IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(CachePipelineLayer.Clustered), nameof(CachePipelineLayer.Clustered));
        var decoratorChain = IndexOfRequired(source, "AddCacheDecoratorChain(", "AddCacheDecoratorChain call");
        var logical = IndexOfRequired(source, "AddLogicalNamespacedCache(", "AddLogicalNamespacedCache call");

        Assert.True(clientCache < ownershipLayer, "ClientCache must register before AddOwnershipGuardLayer.");
        Assert.True(ownershipLayer < clustered, "AddOwnershipGuardLayer must run before AddClusteredCacheSingleton.");
        Assert.True(clustered < decoratorChain, "AddClusteredCacheSingleton must run before AddCacheDecoratorChain.");
        Assert.True(decoratorChain < logical, "AddCacheDecoratorChain must run before AddLogicalNamespacedCache.");
    }

    private static int IndexOfRequired(string source, string token, string description)
    {
        var index = source.IndexOf(token, StringComparison.Ordinal);
        Assert.True(index >= 0, $"{description}: missing `{token}`.");
        return index;
    }
}
