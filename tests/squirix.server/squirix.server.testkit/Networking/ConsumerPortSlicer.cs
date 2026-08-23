using System;
using System.Threading;
using Squirix.Server.TestKit.Diagnostics;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Assigns each test process an exclusive slice of shared host-port regions.</summary>
/// <remarks>
///     <para>
///     Test assemblies run in separate processes when <c>parallelizeAssembly</c> is enabled.
///     <see cref="PortAllocator" /> only coordinates allocations within a single process, so
///     separate processes could otherwise select the same port from shared regions.
///     </para>
///     <para>
///     Each process claims one of <see cref="SliceCount" /> slices using a named system
///     <see cref="Mutex" />. The mutex remains owned for the lifetime of the process, ensuring
///     that no two concurrent test processes use the same slice.
///     </para>
///     <para>
///     If all slices are already claimed, the last slice is reused. This means cross-process
///     port collisions are possible when more than <see cref="SliceCount" /> test processes
///     run concurrently.
///     </para>
/// </remarks>
internal static class ConsumerPortSlicer
{
    internal const int SliceCount = 8;

    private static readonly (int Index, Mutex? Mutex) SliceClaim = ClaimSlice();

    private static readonly int SliceIndex = SliceClaim.Index;

    /// <summary>Returns a <see cref="ListenPortPool" /> for this process's exclusive slice of <paramref name="region" />.</summary>
    /// <param name="region">The shared host port region to slice.</param>
    /// <returns>A port pool backed by this process's disjoint sub-range of <paramref name="region" />.</returns>
    internal static ListenPortPool PoolFor(HostPortRegion region)
    {
        var (start, end) = SliceForIndex(SliceIndex, region);
        return ListenPortPool.ForRange(start, end);
    }

    internal static (int StartInclusive, int EndInclusive) Slice(HostPortRegion region) => SliceForIndex(SliceIndex, region);

    internal static (int StartInclusive, int EndInclusive) SliceForIndex(int index, HostPortRegion region)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, SliceCount);

        var regionStart = HostPortRegions.StartInclusive(region);
        var regionEndInclusive = HostPortRegions.EndExclusive(region) - 1;
        var regionSize = regionEndInclusive - regionStart + 1;
        var sliceSize = regionSize / SliceCount;

        var start = regionStart + (index * sliceSize);
        var end = index == SliceCount - 1 ? regionEndInclusive : start + sliceSize - 1;

        return (start, end);
    }

    private static (int Index, Mutex? Mutex) ClaimSlice()
    {
        for (var index = 0; index < SliceCount; index++)
        {
            Mutex? mutex = null;
            try
            {
                mutex = new Mutex(true, $"squirix-test-port-slice-{index}", out var createdNew);

                if (createdNew)
                    return (index, mutex);
            }
            catch (UnauthorizedAccessException ex)
            {
                TestLog.Suppressed($"Port slice {index} cannot be claimed (unauthorized); trying next.", ex);
            }
            catch (WaitHandleCannotBeOpenedException ex)
            {
                TestLog.Suppressed($"Port slice {index} cannot be claimed (already open); trying next.", ex);
            }

            // Slice already owned by another process (or claim failed): release our handle.
            mutex?.Dispose();
        }

        // More than SliceCount test processes are running concurrently.
        // Reuse the last slice; PortAllocator still guarantees uniqueness within
        // this process, but cross-process collisions are possible.
        return (SliceCount - 1, null);
    }
}
