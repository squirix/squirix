using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Protects the per-test-process disjoint port-slice invariant that keeps cross-assembly parallelism safe.</summary>
public sealed class ConsumerPortSlicerTests
{
    /// <summary>Distinct slices must be non-overlapping so parallel processes never collide on OIDC authority ports.</summary>
    [Test]
    public Task MockOidcAuthoritySlicesAreDisjoint() => AssertSlicesDisjoint(HostPortRegion.MockOidcAuthority);

    /// <summary>Distinct slices must be non-overlapping so parallel processes never collide on mTLS internal ports.</summary>
    [Test]
    public Task MtlsInternalSlicesAreDisjoint() => AssertSlicesDisjoint(HostPortRegion.MtlsInternal);

    /// <summary>Every slice must sit fully inside the shared mTLS internal region.</summary>
    [Test]
    public async Task MtlsInternalSlicesStayWithinRegionBounds()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1;

        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (start, end) = ConsumerPortSlicer.SliceForIndex(i, HostPortRegion.MtlsInternal);
            _ = await Assert.That(start >= regionStart && end <= regionEndInclusive).IsTrue()
                            .Because($"Slice {i} [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
            _ = await Assert.That(end >= start).IsTrue().Because($"Slice {i} is inverted.");
        }
    }

    /// <summary>Every slice must sit fully inside the shared OIDC authority region.</summary>
    [Test]
    public async Task OidcAuthoritySlicesStayInRegionBounds()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MockOidcAuthority);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MockOidcAuthority) - 1;

        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (start, end) = ConsumerPortSlicer.SliceForIndex(i, HostPortRegion.MockOidcAuthority);
            _ = await Assert.That(start >= regionStart && end <= regionEndInclusive).IsTrue()
                            .Because($"Slice {i} [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
            _ = await Assert.That(end >= start).IsTrue().Because($"Slice {i} is inverted.");
        }
    }

    /// <summary>The runtime slice chosen for this process must be a valid in-region range.</summary>
    [Test]
    public async Task RuntimeSliceIsWithinMtlsInternalRegion()
    {
        var regionStart = HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal);
        var regionEndInclusive = HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1;
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.MtlsInternal);
        _ = await Assert.That(start >= regionStart && end <= regionEndInclusive).IsTrue()
                        .Because($"Runtime slice [{start}..{end}] left region [{regionStart}..{regionEndInclusive}].");
    }

    /// <summary>A held exclusive slice lock must reject a second claim, restoring the cross-process guarantee the unreliable named mutex failed to provide on Linux.</summary>
    [Test]
    public void SliceLockFileExcludesConcurrentClaim()
    {
        var lockPath = Path.Join(Path.GetTempPath(), $"squirix-testkit-slice-lock-{Guid.NewGuid():N}.tmp");

        try
        {
            using var held = File.OpenHandle(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            _ = NodeExceptionAssert.For<IOException>().Throws(lockPath, static path => _ = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
        }
        finally
        {
            File.Delete(lockPath);
        }
    }

    private static async Task AssertSlicesDisjoint(HostPortRegion region)
    {
        for (var i = 0; i < ConsumerPortSlicer.SliceCount; i++)
        {
            var (startA, endA) = ConsumerPortSlicer.SliceForIndex(i, region);
            for (var j = i + 1; j < ConsumerPortSlicer.SliceCount; j++)
            {
                var (startB, endB) = ConsumerPortSlicer.SliceForIndex(j, region);
                var overlaps = startA <= endB && startB <= endA;
                _ = await Assert.That(overlaps).IsFalse().Because($"Slices {i} [{startA}..{endA}] and {j} [{startB}..{endB}] overlap in region {region}.");
            }
        }
    }
}
