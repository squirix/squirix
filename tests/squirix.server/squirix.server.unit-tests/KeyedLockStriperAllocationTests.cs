using Squirix.Server.LocalCache;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Allocation-focused tests for keyed lock striping.
/// </summary>
public sealed class KeyedLockStriperAllocationTests : ServerUnitTestBase
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
