using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.TestKit.Diagnostics;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Assigns each test process an exclusive slice of shared host-port regions.</summary>
/// <remarks>
///     <para>
///     Test assemblies run in separate processes when <c language="csharp">parallelizeAssembly</c> is enabled.
///     <see cref="PortAllocator" /> only coordinates allocations within a single process, so
///     separate processes could otherwise select the same port from shared regions.
///     </para>
///     <para>
///     Each process claims one of <see cref="SliceCount" /> slices by holding a slicing lock file
///     open with <see cref="FileShare.None" /> for the lifetime of the process. On Linux this is an
///     advisory <c language="csharp">flock</c> exclusive lock, which is reliable across processes
///     (unlike named <c language="csharp">Mutex</c> instances, which are not). The operating system
///     releases the lock automatically if the claiming process exits or crashes.
///     </para>
///     <para>
///     If all slices are already claimed, the process fails with an explicit capacity error
///     instead of silently reusing an unlocked slice, preserving disjoint port ownership.
///     </para>
/// </remarks>
internal static class ConsumerPortSlicer
{
    internal const int SliceCount = 8;

    private static readonly (int Index, SafeFileHandle Lock) SliceClaim = ClaimSlice();

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

    private static (int Index, SafeFileHandle Lock) ClaimSlice()
    {
        for (var index = 0; index < SliceCount; index++)
        {
            var lockStream = TryOpenSliceLock(index);
            if (lockStream != null)
                return (index, lockStream);
        }

        // More than SliceCount test processes are running concurrently. Do not reuse a locked
        // slice: that would let two processes collide on the same ports. Fail loud instead.
        throw new InvalidOperationException(
            $"All {SliceCount} test port slices are claimed by concurrent processes. Reduce test " +
            $"parallelism to {SliceCount} or fewer concurrent test processes.");
    }

    /// <summary>Attempts to claim a slice by opening its lock file with exclusive sharing.</summary>
    /// <param name="sliceIndex">Index of the slice to claim.</param>
    /// <returns>The held lock handle, or <see langword="null" /> when another process owns the slice.</returns>
    private static SafeFileHandle? TryOpenSliceLock(int sliceIndex)
    {
        try
        {
            var lockDirectory = Path.Join(Path.GetTempPath(), "squirix-testkit-port-slices");
            _ = Directory.CreateDirectory(lockDirectory);

            var lockFilePath = Path.Join(lockDirectory, $"squirix-test-port-slice-{sliceIndex}.lock");
            return File.OpenHandle(lockFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (UnauthorizedAccessException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} cannot be claimed (unauthorized); trying next.", ex);
        }
        catch (IOException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} cannot be claimed (already open); trying next.", ex);
        }

        return null;
    }
}
