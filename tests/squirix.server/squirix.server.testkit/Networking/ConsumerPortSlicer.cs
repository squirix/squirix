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
///     <para>
///     Among free slices the least-recently-claimed one wins, tracked by each lock
///     file's writing time, so a new process reuses a long-idle range instead of one
///     whose sockets may still linger after the previous owner exited.
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

    /// <summary>Orders slice indexes oldest-claim-first, so quarantine prefers long-idle ranges.</summary>
    /// <param name="candidates">Slice indexes to order.</param>
    /// <param name="readLastClaimTicks">Reads the last-claim timestamp ticks per slice, or <see langword="null" /> when never claimed.</param>
    /// <returns>The candidate indexes, never-claimed and oldest first, unreadable last, tie-keeping index order.</returns>
    internal static int[] OrderByLastClaim(int[] candidates, Func<int, long?> readLastClaimTicks)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(readLastClaimTicks);

        var keys = new long?[candidates.Length];
        for (var i = 0; i < candidates.Length; i++)
            keys[i] = readLastClaimTicks(candidates[i]);

        var order = new int[candidates.Length];
        var used = new bool[candidates.Length];
        for (var k = 0; k < candidates.Length; k++)
        {
            var best = -1;
            for (var i = 0; i < candidates.Length; i++)
            {
                if (!used[i] && (best < 0 || CompareClaim(keys[i], keys[best]) < 0))
                    best = i;
            }

            used[best] = true;
            order[k] = candidates[best];
        }

        return order;
    }

    private static (int Index, SafeFileHandle Lock) ClaimSlice()
    {
        var candidates = new int[SliceCount];
        for (var i = 0; i < SliceCount; i++)
            candidates[i] = i;

        foreach (var index in OrderByLastClaim(candidates, ReadLastClaimTicks))
        {
            SafeFileHandle? lockStream = null;
            try
            {
                lockStream = OpenSliceLock(index);
                if (lockStream == null)
                    continue;

                TouchClaimMarker(index, lockStream);
                var claimed = lockStream;
                lockStream = null;
                return (index, claimed);
            }
            finally
            {
                lockStream?.Dispose();
            }
        }

        // More than SliceCount test processes are running concurrently. Do not reuse a locked
        // slice: that would let two processes collide on the same ports. Fail loud instead.
        throw new InvalidOperationException(
            $"All {SliceCount} test port slices are claimed by concurrent processes. Reduce test " +
            $"parallelism to {SliceCount} or fewer concurrent test processes.");
    }

    private static int CompareClaim(long? left, long? right)
    {
        var leftTicks = left ?? long.MinValue;
        var rightTicks = right ?? long.MinValue;
        return leftTicks.CompareTo(rightTicks);
    }

    /// <summary>Attempts to claim a slice by opening its lock file with exclusive sharing.</summary>
    /// <param name="sliceIndex">Index of the slice to claim.</param>
    /// <returns>The held lock handle, or <see langword="null" /> when another process owns the slice.</returns>
    private static SafeFileHandle? OpenSliceLock(int sliceIndex)
    {
        try
        {
            _ = Directory.CreateDirectory(SliceLockDirectory());

            var lockFilePath = SliceLockPath(sliceIndex);
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

    private static long? ReadLastClaimTicks(int sliceIndex)
    {
        try
        {
            var lockFilePath = SliceLockPath(sliceIndex);
            return !File.Exists(lockFilePath) ? null : File.GetLastWriteTimeUtc(lockFilePath).Ticks;
        }
        catch (IOException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} claim time cannot be read; trying it last.", ex);
            return long.MaxValue;
        }
        catch (UnauthorizedAccessException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} claim time cannot be read; trying it last.", ex);
            return long.MaxValue;
        }
    }

    private static string SliceLockDirectory() => Path.Join(Path.GetTempPath(), "squirix-testkit-port-slices");

    private static string SliceLockPath(int sliceIndex) => Path.Join(SliceLockDirectory(), $"squirix-test-port-slice-{sliceIndex}.lock");

    private static void TouchClaimMarker(int sliceIndex, SafeFileHandle lockStream)
    {
        try
        {
            File.SetLastWriteTimeUtc(lockStream, DateTime.UtcNow);
        }
        catch (IOException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} claim time cannot be written; slice order degrades to index order.", ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            TestLog.Suppressed($"Port slice {sliceIndex} claim time cannot be written; slice order degrades to index order.", ex);
        }
    }
}
