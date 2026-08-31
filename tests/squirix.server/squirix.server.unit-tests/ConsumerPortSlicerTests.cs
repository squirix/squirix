using System;
using System.IO;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Protects the per-test-process disjoint port-slice invariant that keeps cross-assembly parallelism safe.</summary>
public sealed class ConsumerPortSlicerTests
{
    /// <summary>Every slice must sit fully inside the shared mTLS internal region.</summary>
    [Fact]
    public void MtlsInternalSlicesStayWithinRegionBounds()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1;

        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (start, end) = ConsumerPortSlicer.SliceForIndex(i, HostPortRegion.MtlsInternal);
            Assert.True(start >= regionStart && end <= regionEndInclusive, $"Slice {i} [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
            Assert.True(end >= start, $"Slice {i} is inverted.");
        }
    }

    /// <summary>Every slice must sit fully inside the shared OIDC authority region.</summary>
    [Fact]
    public void OidcAuthoritySlicesStayInRegionBounds()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MockOidcAuthority);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MockOidcAuthority) - 1;

        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (start, end) = ConsumerPortSlicer.SliceForIndex(i, HostPortRegion.MockOidcAuthority);
            Assert.True(start >= regionStart && end <= regionEndInclusive, $"Slice {i} [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
            Assert.True(end >= start, $"Slice {i} is inverted.");
        }
    }

    /// <summary>Distinct slices must be non-overlapping so parallel processes never collide on mTLS internal ports.</summary>
    [Fact]
    public void MtlsInternalSlicesAreDisjoint() => AssertSlicesDisjoint(HostPortRegion.MtlsInternal);

    /// <summary>Distinct slices must be non-overlapping so parallel processes never collide on OIDC authority ports.</summary>
    [Fact]
    public void MockOidcAuthoritySlicesAreDisjoint() => AssertSlicesDisjoint(HostPortRegion.MockOidcAuthority);

    /// <summary>The runtime slice chosen for this process must be a valid in-region range.</summary>
    [Fact]
    public void RuntimeSliceIsWithinMtlsInternalRegion()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1;
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.MtlsInternal);
        Assert.True(start >= regionStart && end <= regionEndInclusive, $"Runtime slice [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
    }

    /// <summary>A held exclusive slice lock must reject a second claim, restoring the cross-process guarantee the unreliable named mutex failed to provide on Linux.</summary>
    [Fact]
    public void SliceLockFileExcludesConcurrentClaim()
    {
        var lockPath = Path.Join(Path.GetTempPath(), $"squirix-testkit-slice-lock-{Guid.NewGuid():N}.tmp");

        try
        {
            var held = File.OpenHandle(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

            try
            {
                _ = NodeExceptionAssert.For<IOException>().Throws(lockPath, static path => _ = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
            }
            finally
            {
                held.Dispose();
            }
        }
        finally
        {
            File.Delete(lockPath);
        }
    }

    private static void AssertSlicesDisjoint(HostPortRegion region)
    {
        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (startA, endA) = ConsumerPortSlicer.SliceForIndex(i, region);
            for (var j = i + 1; j < ConsumerPortSlicer.SliceCount; j++)
            {
                var (startB, endB) = ConsumerPortSlicer.SliceForIndex(j, region);
                var overlaps = startA <= endB && startB <= endA;
                Assert.False(overlaps, $"Slices {i} [{startA}..{endA}] and {j} [{startB}..{endB}] overlap in region {region}.");
            }
        }
    }
}
