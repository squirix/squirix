namespace Squirix.Server.TestKit.Networking;

/// <summary>
/// Sequential, equal-sized port regions for in-process test hosts and auxiliary listeners.
/// </summary>
/// <remarks>
/// Each region spans <see cref="RegionSize" /> consecutive ports starting at <see cref="Origin" />.
/// </remarks>
internal static class HostPortRegions
{
    private const int Origin = 20_000;
    private const int RegionSize = 2_000;

    internal static int EndExclusive(HostPortRegion region) => StartInclusive(region) + RegionSize;

    internal static int StartInclusive(HostPortRegion region) => Origin + ((int)region * RegionSize);
}
