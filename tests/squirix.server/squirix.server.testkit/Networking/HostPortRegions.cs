using System;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Sequential, equal-sized port regions for in-process test hosts and auxiliary listeners.</summary>
/// <remarks>
/// Each region spans <see cref="RegionSize" /> consecutive ports starting at <see cref="Origin" />.
/// </remarks>
internal static class HostPortRegions
{
    private const int Origin = 20_000;
    private const int RegionSize = 2_000;

    internal static int EndExclusive(HostPortRegion region) => StartInclusive(region) + RegionSize;

    internal static int StartInclusive(HostPortRegion region) => Origin + (RegionIndex(region) * RegionSize);

    private static int RegionIndex(HostPortRegion region) => region switch
    {
        HostPortRegion.EndToEndBenchmarks => 0,
        HostPortRegion.EndToEndTests => 1,
        HostPortRegion.SmokeTests => 2,
        HostPortRegion.IntegrationTests => 3,
        HostPortRegion.ServerBenchmarks => 4,
        HostPortRegion.MockOidcAuthority => 5,
        HostPortRegion.MtlsInternal => 6,
        HostPortRegion.ServerUnitTests => 7,
        _ => throw new ArgumentOutOfRangeException(nameof(region), region, "Unsupported host port region."),
    };
}
