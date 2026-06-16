using Squirix.Server.LocalCache;
using Squirix.Server.TestKit.Testing;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Allocation-focused tests for keyed lock striping.
/// </summary>
public sealed class KeyedLockStriperAllocationTests : UnitTestBase
{
    /// <summary>
    /// Verifies that <c>KeyedLockStriper.AcquireAll</c> remains allocation-free on the hot batch path.
    /// </summary>
    [Fact]
    public void AcquireAllDoesNotAllocate()
    {
        var striper = new KeyedLockStriper();
        string[] keys = ["orders:1", "orders:2", "orders:3", "orders:4"];

        for (var i = 0; i < 1_000; i++)
        {
            using var warmup = striper.AcquireAll(keys);
        }

        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            for (var i = 0; i < 10_000; i++)
            {
                using var releaser = striper.AcquireAll(keys);
            }
        });

        Assert.Equal(0, allocated);
    }
}
