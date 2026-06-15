using System;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Shared TestKit port pool for e2e host fixtures.
/// </summary>
internal static class ListenPortPool
{
    private const int RangeStart = 40_000;
    private const int RangeSize = 200;
    private const int RangeEndCap = 65_535;

    private static readonly PortAllocator Pool = CreatePool();

    internal static string NextHttpUrl() => $"https://127.0.0.1:{Pool.Allocate()}";

    private static PortAllocator CreatePool()
    {
        var maxBuckets = Math.Max(1, (RangeEndCap - RangeStart) / RangeSize);
        var salt = Environment.ProcessId % maxBuckets;
        var start = RangeStart + (salt * RangeSize);
        return new PortAllocator(start, start + RangeSize - 1);
    }
}
