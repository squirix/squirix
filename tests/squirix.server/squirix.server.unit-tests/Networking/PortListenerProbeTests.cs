using System;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Networking;

/// <summary>The port listener probe fails while a socket listens on the port and passes once it is closed.</summary>
public sealed class PortListenerProbeTests
{
    /// <summary>A live listener fails the probe, which names the port; the probe passes once the listener is stopped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProbeFailsWhileListening(CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = KitThrowHelper.Required(listener.LocalEndpoint as IPEndPoint, "The listener has no IP endpoint.").Port;
        InvalidOperationException failure;
        try
        {
            failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(PortListenerProbe.AssertNoListenerAsync(port, cancellationToken));
        }
        finally
        {
            listener.Stop();
        }

        _ = await Assert.That(failure.Message).Contains(port.ToString(CultureInfo.InvariantCulture));
        await PortListenerProbe.AssertNoListenerAsync(port, cancellationToken);
    }
}
