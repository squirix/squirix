using System;
using System.Globalization;
using System.Threading;

namespace Squirix.Server.TestKit.Networking;

/// <summary>
/// Process-scoped HTTPS listen port pools backed by <see cref="PortAllocator" />.
/// </summary>
/// <remarks>
/// Each preset allocates from the full <see cref="HostPortRegion" /> range. Cross-process safety
/// relies on bind probing in <see cref="PortAllocator" />; regions stay disjoint per consumer.
/// </remarks>
public sealed class ListenPortPool : IDisposable
{
    private readonly PortAllocator _allocator;
    private int _disposed;

    private ListenPortPool(HostPortRegion region)
    {
        var regionStart = HostPortRegions.StartInclusive(region);
        var regionEndInclusive = HostPortRegions.EndExclusive(region) - 1;
        _allocator = new PortAllocator(regionStart, regionEndInclusive);
    }

    private ListenPortPool(int startInclusive, int endInclusive)
    {
        _allocator = new PortAllocator(startInclusive, endInclusive);
    }

    /// <summary>Gets the port pool for end-to-end BenchmarkDotNet hosts.</summary>
    public static ListenPortPool EndToEndBenchmarks { get; } = new(HostPortRegion.EndToEndBenchmarks);

    /// <summary>Gets the port pool for end-to-end SDK test hosts.</summary>
    public static ListenPortPool EndToEndTests { get; } = new(HostPortRegion.EndToEndTests);

    /// <summary>Gets the port pool for server integration test hosts.</summary>
    public static ListenPortPool IntegrationTests { get; } = new(HostPortRegion.IntegrationTests);

    /// <summary>Gets the port pool for in-process server pipeline benchmarks.</summary>
    public static ListenPortPool ServerBenchmarks { get; } = new(HostPortRegion.ServerBenchmarks);

    /// <summary>Gets the port pool for server unit tests that bind HTTPS listeners.</summary>
    public static ListenPortPool ServerUnitTests { get; } = new(HostPortRegion.ServerUnitTests);

    /// <summary>Gets the port pool for server smoke test hosts.</summary>
    public static ListenPortPool SmokeTests { get; } = new(HostPortRegion.SmokeTests);

    /// <summary>Reserves the next free port from this pool.</summary>
    /// <returns>A loopback port number.</returns>
    public int AllocatePort() => _allocator.Allocate();

    /// <summary>Reserves the next free port and returns a loopback HTTPS listen URI.</summary>
    /// <returns>A URI of the form <c>https://127.0.0.1:&lt;port&gt;</c>.</returns>
    public Uri NextHttpUri() => new(NextHttpAddress(), UriKind.Absolute);

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _allocator.Dispose();
    }

    /// <summary>Builds a pool over an explicit inclusive port range (used for per-process shared-region slices).</summary>
    /// <param name="startInclusive">Inclusive lower bound of the port range.</param>
    /// <param name="endInclusive">Inclusive upper bound of the port range.</param>
    /// <returns>A port pool backed by the given range.</returns>
    internal static ListenPortPool ForRange(int startInclusive, int endInclusive) => new(startInclusive, endInclusive);

    private static string FormatLoopbackHttps(int port) => string.Create(CultureInfo.InvariantCulture, $"https://127.0.0.1:{port}");

    /// <summary>Reserves the next free port and returns a canonical loopback HTTPS listen URL.</summary>
    /// <returns>A URL of the form <c>https://127.0.0.1:&lt;port&gt;</c>.</returns>
    private string NextHttpAddress() => FormatLoopbackHttps(AllocatePort());
}
