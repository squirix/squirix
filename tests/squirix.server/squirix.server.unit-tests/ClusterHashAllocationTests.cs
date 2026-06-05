using Squirix.Server.Cluster;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Allocation-focused tests for cluster hashing hot paths.
/// </summary>
public sealed class ClusterHashAllocationTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies that <c>Sha256Hasher.HashString</c> stays allocation-free for ASCII input on the hot path.
    /// </summary>
    [Fact]
    public void Sha256HasherHashStringAsciiDoesNotAllocate()
    {
        var hasher = new Sha256Hasher();
        const string text = "orders:customer:42:active";

        _ = hasher.HashString(text);

        var sum = 0UL;
        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            for (var i = 0; i < 10_000; i++)
                sum += hasher.HashString(text);
        });

        Assert.NotEqual(0UL, sum);
        Assert.Equal(0, allocated);
    }

    /// <summary>
    /// Verifies that <c>Sha256Hasher.HashString</c> stays allocation-free for UTF-8 input on the hot path.
    /// </summary>
    [Fact]
    public void Sha256HasherHashStringUtf8DoesNotAllocate()
    {
        var hasher = new Sha256Hasher();
        const string text = "key:order:42:active";

        _ = hasher.HashString(text);

        var sum = 0UL;
        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            for (var i = 0; i < 10_000; i++)
                sum += hasher.HashString(text);
        });

        Assert.NotEqual(0UL, sum);
        Assert.Equal(0, allocated);
    }
}
