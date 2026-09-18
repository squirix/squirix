using System;
using System.Globalization;
using System.Threading;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Process-scoped HTTPS listen port pools backed by <see cref="PortAllocator" />.</summary>
/// <remarks>
/// Each preset allocates from this process's exclusive slice of its <see cref="HostPortRegion" />,
/// claimed via <see cref="ConsumerPortSlicer" />, so concurrently running processes of the same
/// consumer use disjoint sub-ranges. Bind probing in <see cref="PortAllocator" /> remains a second
/// line of defense against residual cross-process races.
/// </remarks>
public sealed class ListenPortPool : IDisposable
{
    private readonly PortAllocator _allocator;
    private int _disposed;

    private ListenPortPool(HostPortRegion region)
    {
        var (sliceStartInclusive, sliceEndInclusive) = ConsumerPortSlicer.Slice(region);
        _allocator = new PortAllocator(sliceStartInclusive, sliceEndInclusive);
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

    /// <summary>Reserves the next free port and holds it bound until the returned handle is disposed of.</summary>
    /// <returns>A held loopback port; disposing it releases the hold for the real bind.</returns>
    public HeldPort HoldPort()
    {
        var port = _allocator.ReserveOne();
        return new HeldPort(this, port, new Uri(FormatLoopbackHttps(port), UriKind.Absolute));
    }

    /// <summary>Reserves a contiguous range of free ports and holds them all bound until each handle is disposed of.</summary>
    /// <param name="count">Number of consecutive free ports to reserve.</param>
    /// <returns>The reserved ports, each bound and held open until its handle is disposed of.</returns>
    public HeldPort[] HoldPorts(int count)
    {
        var ports = _allocator.ReserveRange(count);
        var held = new HeldPort[ports.Length];
        for (var i = 0; i < ports.Length; i++)
            held[i] = new HeldPort(this, ports[i], new Uri(FormatLoopbackHttps(ports[i]), UriKind.Absolute));

        return held;
    }

    /// <summary>Reserves the next free port, holds it bound, and returns a loopback HTTPS listen URI.</summary>
    /// <returns>A URI of the form <c language="csharp">https://127.0.0.1:&lt;port&gt;</c>, held open until released via <see cref="ReleasePort" />.</returns>
    public Uri HoldHttpUri() => new(FormatLoopbackHttps(_allocator.ReserveOne()), UriKind.Absolute);

    /// <summary>Releases a previously reserved port so the actual server can bind to it.</summary>
    /// <param name="port">The port number to release.</param>
    public void ReleasePort(int port) => _allocator.ReleasePort(port);

    /// <summary>Releases a held primary port on whichever pool owns it; ignores foreign ports.</summary>
    /// <param name="uri">The listen URI whose port to release.</param>
    /// <remarks>
    /// Node startup serves tests and benchmarks from different pools without knowing which one
    /// issued the URI. Pool ranges are disjoint per process, so at most one pool matches; anything else
    /// (hardcoded ports, other regions) is a no-op, exactly like <see cref="ReleasePort" /> for unheld ports.
    /// </remarks>
    public static void ReleaseHeldPrimary(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (EndToEndTests.Owns(uri.Port))
            EndToEndTests.ReleasePort(uri.Port);
        else if (EndToEndBenchmarks.Owns(uri.Port))
            EndToEndBenchmarks.ReleasePort(uri.Port);
        else if (IntegrationTests.Owns(uri.Port))
            IntegrationTests.ReleasePort(uri.Port);
        else if (SmokeTests.Owns(uri.Port))
            SmokeTests.ReleasePort(uri.Port);
        else if (ServerBenchmarks.Owns(uri.Port))
            ServerBenchmarks.ReleasePort(uri.Port);
        else if (ServerUnitTests.Owns(uri.Port))
            ServerUnitTests.ReleasePort(uri.Port);
    }

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

    /// <summary>Binds and holds a specific port previously allocated from this pool.</summary>
    /// <param name="port">The port number to hold.</param>
    /// <returns>The held port when the bind succeeds; otherwise <see langword="null" />.</returns>
    internal HeldPort? HoldSpecificPort(int port) =>
        _allocator.TryHoldSpecific(port) ? new HeldPort(this, port, new Uri(FormatLoopbackHttps(port), UriKind.Absolute)) : null;

    private bool Owns(int port) => _allocator.Contains(port);

    private static string FormatLoopbackHttps(int port) => string.Create(CultureInfo.InvariantCulture, $"https://127.0.0.1:{port}");
}
