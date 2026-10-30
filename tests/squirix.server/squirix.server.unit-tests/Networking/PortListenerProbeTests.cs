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

    /// <summary>A reset is retried; only a subsequent refusal proves that the listener is gone.</summary>
    /// <param name="result">The result after the initial reset.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(SocketError.ConnectionRefused)]
    [Arguments(SocketError.Success)]
    [Arguments(SocketError.AccessDenied)]
    public async Task ResetRequiresAnotherProbe(SocketError result, CancellationToken cancellationToken)
    {
        var attempts = 0;
        var failure = await PortListenerProbe.ProbeConnectionAsync(
            1,
            (_, _, _) =>
            {
                var error = ++attempts == 1 ? SocketError.ConnectionReset : result;
                return error == SocketError.Success ? ValueTask.CompletedTask : ValueTask.FromException(new SocketException(Convert.ToInt32(error, CultureInfo.InvariantCulture)));
            },
            cancellationToken);

        _ = await Assert.That(attempts).IsEqualTo(2);
        if (result == SocketError.ConnectionRefused)
            _ = await Assert.That(failure).IsNull();
        else
            _ = await Assert.That(failure).IsNotNull();
    }

    /// <summary>Cancellation during a reset retry is propagated rather than accepted as a closed listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResetRetryHonorsCancellation(CancellationToken cancellationToken)
    {
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var probe = PortListenerProbe.ProbeConnectionAsync(
            1,
            async (_, _, _) =>
            {
                await canceled.CancelAsync();
                throw new SocketException(Convert.ToInt32(SocketError.ConnectionReset, CultureInfo.InvariantCulture));
            },
            canceled.Token);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(probe);
    }
}
