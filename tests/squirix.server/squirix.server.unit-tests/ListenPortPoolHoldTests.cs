using System;
using System.Net;
using System.Net.Sockets;
using Squirix.Server.TestKit.Networking;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Pins pool-agnostic release routing used by in-process node startup, which serves pools it cannot name.</summary>
public sealed class ListenPortPoolHoldTests
{
    /// <summary>A held port must become bindable after fan-out release, and re-release must stay a no-op.</summary>
    [Test]
    public void ReleaseHeldPrimaryReleasesOwningPool()
    {
        using var held = ListenPortPool.IntegrationTests.HoldPort();
        ListenPortPool.ReleaseHeldPrimary(held.HttpUri);
        BindExclusively(held.Port);
        ListenPortPool.ReleaseHeldPrimary(held.HttpUri);
    }

    /// <summary>Ports outside every pool range must pass through fan-out silently.</summary>
    [Test]
    public void ReleaseHeldPrimaryIgnoresForeignUri() =>
        ListenPortPool.ReleaseHeldPrimary(new Uri("https://127.0.0.1:7000", UriKind.Absolute));

    private static void BindExclusively(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
        listener.Start();
        listener.Stop();
    }
}
