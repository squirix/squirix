using System;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Architecture;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Shared assertions for <see cref="SquirixCachePipelineRegistration" /> source order vs <see cref="CachePipelineDescriptor" />.
/// </summary>
internal static class CachePipelineRegistrationOrderAssertions
{
    /// <summary>
    /// Asserts singleton markers appear in <see cref="CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter" /> order.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    public static void AssertSingletonOrderMatchesDescriptor(string source)
    {
        var order = CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter;
        Assert.True(order.Length >= 2, "Descriptor must list at least two singleton layers.");

        for (var i = 0; i < order.Length - 1; i++)
        {
            var outerLater = order[i + 1];
            var innerEarlier = order[i];
            var innerMarker = CachePipelineDescriptor.AddSingletonRegistrationMarker(innerEarlier);
            var outerMarker = CachePipelineDescriptor.AddSingletonRegistrationMarker(outerLater);
            var innerIndex = IndexOfRequired(source, innerMarker, innerEarlier.ToString());
            var outerIndex = IndexOfRequired(source, outerMarker, outerLater.ToString());
            Assert.True(innerIndex < outerIndex, $"{innerEarlier} must appear before {outerLater} in registration source.");
        }
    }

    /// <summary>
    /// Asserts the logical aggregate registration resolves tracing after the tracing singleton is registered.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    public static void AssertTracingPrecedesLogicalAggregate(string source)
    {
        var tracing = IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(CachePipelineLayer.Tracing), nameof(CachePipelineLayer.Tracing));
        var logical = IndexOfRequired(source, CachePipelineDescriptor.LogicalAggregateRegistrationMarker, "ILogicalNamespacedCache registration");
        Assert.True(logical > tracing, "ILogicalNamespacedCache must resolve after TracingCacheDecorator registration.");
    }

    /// <summary>
    /// Loads the pipeline registration source file from the repository layout.
    /// </summary>
    /// <returns>File contents.</returns>
    public static string LoadPipelineRegistrationSource() =>
        ArchitectureRepositoryPaths.ReadSquirixLibrarySource(PathKit.Combine("Node", "Hosting", "SquirixCachePipelineRegistration.cs"));

    /// <summary>
    /// Returns the index of the <see cref="CachePipelineDescriptor.AddSingletonRegistrationMarker" /> for <paramref name="layer" />.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    /// <param name="layer">The pipeline layer.</param>
    /// <returns>The marker index.</returns>
    internal static int IndexOfRequired(string source, CachePipelineLayer layer) =>
        IndexOfRequired(source, CachePipelineDescriptor.AddSingletonRegistrationMarker(layer), layer.ToString());

    /// <summary>
    /// Returns the zero-based index of a required substring in registration source, or fails the test if it is missing.
    /// </summary>
    /// <param name="source">Registration source text.</param>
    /// <param name="token">Substring that must appear exactly once for ordering checks.</param>
    /// <param name="description">Human-readable context for assertion failures.</param>
    /// <returns>The index of <paramref name="token" />.</returns>
    private static int IndexOfRequired(string source, string token, string description)
    {
        var index = source.IndexOf(token, StringComparison.Ordinal);
        Assert.True(index >= 0, $"{description}: missing `{token}`.");
        return index;
    }
}
