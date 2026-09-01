using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for <see cref="DistinctNodeIds" />.</summary>
[Immutable]
public sealed class DistinctNodeIdsTests : ServerUnitTestBase
{
    /// <summary>Leading and trailing whitespace is trimmed from node IDs.</summary>
    [Fact]
    public void TrimsLeadingAndTrailingWhitespace()
    {
        var result = DistinctNodeIds.InInsertionOrder([" NodeA ", "NodeB"]);
        Assert.Equal(["NodeA", "NodeB"], result);
    }

    /// <summary>Whitespace-only values are rejected after trimming.</summary>
    [Fact]
    public void RejectsWhitespaceOnlyValues()
    {
        var result = DistinctNodeIds.InInsertionOrder(["   ", "NodeA", "\t\n"]);
        Assert.Equal(["NodeA"], result);
    }

    /// <summary>Duplicate values after trimming are deduplicated.</summary>
    [Fact]
    public void DeduplicatesAfterTrim()
    {
        var result = DistinctNodeIds.InInsertionOrder([" NodeA ", "NodeA", "  NodeA  "]);
        Assert.Single(result, "NodeA");
    }

    /// <summary>Preserves insertion order of first-seen distinct IDs.</summary>
    [Fact]
    public void PreservesInsertionOrder()
    {
        var result = DistinctNodeIds.InInsertionOrder(["NodeC", " NodeA ", "NodeB", "NodeA"]);
        Assert.Equal(["NodeC", "NodeA", "NodeB"], result);
    }

    /// <summary>Empty input returns empty array.</summary>
    [Fact]
    public void EmptyInputReturnsEmpty()
    {
        var result = DistinctNodeIds.InInsertionOrder([]);
        Assert.Empty(result);
    }

    /// <summary>All whitespace values returns empty array.</summary>
    [Fact]
    public void AllWhitespaceReturnsEmpty()
    {
        var result = DistinctNodeIds.InInsertionOrder(["  ", "\t", string.Empty]);
        Assert.Empty(result);
    }
}
