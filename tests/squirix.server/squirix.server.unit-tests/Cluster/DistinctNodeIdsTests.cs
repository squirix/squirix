using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for <see cref="DistinctNodeIds" />.</summary>
[Immutable]
public sealed class DistinctNodeIdsTests : ServerUnitTestBase
{
    /// <summary>All whitespace values returns empty array.</summary>
    [Test]
    public async Task AllWhitespaceReturnsEmpty()
    {
        var result = DistinctNodeIds.InInsertionOrder(["  ", "\t", string.Empty]);
        _ = await Assert.That(result).IsEmpty();
    }

    /// <summary>Duplicate values after trimming are deduplicated.</summary>
    [Test]
    public Task DeduplicatesAfterTrim() => SequenceAssert.EqualAsync(["NodeA"], DistinctNodeIds.InInsertionOrder([" NodeA ", "NodeA", "  NodeA  "]), StringComparer.Ordinal);

    /// <summary>Empty input returns empty array.</summary>
    [Test]
    public async Task EmptyInputReturnsEmpty()
    {
        var result = DistinctNodeIds.InInsertionOrder([]);
        _ = await Assert.That(result).IsEmpty();
    }

    /// <summary>Preserves insertion order of first-seen distinct IDs.</summary>
    [Test]
    public Task PreservesInsertionOrder() => SequenceAssert.EqualAsync(
        ["NodeC", "NodeA", "NodeB"],
        DistinctNodeIds.InInsertionOrder(["NodeC", " NodeA ", "NodeB", "NodeA"]),
        StringComparer.Ordinal);

    /// <summary>Whitespace-only values are rejected after trimming.</summary>
    [Test]
    public Task RejectsWhitespaceOnlyValues() => SequenceAssert.EqualAsync(["NodeA"], DistinctNodeIds.InInsertionOrder(["   ", "NodeA", "\t\n"]), StringComparer.Ordinal);

    /// <summary>Leading and trailing whitespace is trimmed from node IDs.</summary>
    [Test]
    public Task TrimsLeadingAndTrailingWhitespace() =>
        SequenceAssert.EqualAsync(["NodeA", "NodeB"], DistinctNodeIds.InInsertionOrder([" NodeA ", "NodeB"]), StringComparer.Ordinal);
}
