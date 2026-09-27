using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Pins that releasing a held port returns only once the port can be bound, so a bind right after the release cannot fail on a system that frees a closed listener late (issue 738).</summary>
public sealed class PortAllocatorReleaseTests
{
    /// <summary>The release polls until the closed hold's port reports bindable, then returns.</summary>
    [Test]
    public async Task ReleaseWaitsUntilPortIsBindable()
    {
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.ServerUnitTests);
        var checks = 0;
        using var allocator = new PortAllocator(start, end, _ => ++checks > 3, TimeSpan.FromSeconds(30));
        var port = allocator.ReserveOne();

        allocator.ReleasePort(port);

        _ = await Assert.That(checks).IsEqualTo(4);
        BindExclusively(port);
    }

    /// <summary>A port that stays busy does not hang the release: it returns after the bounded wait so the real bind reports the failure.</summary>
    [Test]
    public async Task ReleaseGivesUpWhenPortStaysBusy()
    {
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.ServerUnitTests);
        var checks = 0;
        var settleTimeout = TimeSpan.FromMilliseconds(150);
        using var allocator = new PortAllocator(start, end, _ => ++checks < 0, settleTimeout);
        var port = allocator.ReserveOne();

        var started = Stopwatch.GetTimestamp();
        allocator.ReleasePort(port);

        _ = await Assert.That(Stopwatch.GetElapsedTime(started) >= settleTimeout).IsTrue();
        _ = await Assert.That(checks > 1).IsTrue();
    }

    /// <summary>Releasing a port that is not held is ignored without probing it.</summary>
    [Test]
    public async Task ReleaseOfUnheldPortDoesNotProbe()
    {
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.ServerUnitTests);
        var checks = 0;
        using var allocator = new PortAllocator(start, end, _ => ++checks > 0, TimeSpan.FromSeconds(30));

        allocator.ReleasePort(start);

        _ = await Assert.That(checks).IsEqualTo(0);
    }

    /// <summary>With the real check, a port is bindable at once after its release, and the check leaves it bindable for the real bind.</summary>
    [Test]
    public void ReleaseLeavesPortBindable()
    {
        var (start, end) = ConsumerPortSlicer.Slice(HostPortRegion.ServerUnitTests);
        using var allocator = new PortAllocator(start, end);
        var port = allocator.ReserveOne();

        allocator.ReleasePort(port);

        BindExclusively(port);
        BindExclusively(port);
    }

    private static void BindExclusively(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
        listener.Start();
        listener.Stop();
    }
}
