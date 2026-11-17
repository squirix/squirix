using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Checks socket ownership and cleanup independently of server-library test infrastructure.</summary>
[Property("Suite", "Smoke")]
public sealed class ProcessPortTests
{
    /// <summary>A reservation holds its socket until the handoff, and disposal closes an unused hold.</summary>
    [Test]
    public async Task ReservationHoldsSocketUntilHandoff()
    {
        using var reservation = ProcessPortReservation.Reserve();
        _ = await Assert.That(CanBind(reservation.Uri.Port)).IsFalse();

        reservation.ReleaseForStart();

        _ = await Assert.That(CanBind(reservation.Uri.Port)).IsTrue();
        using var unused = ProcessPortReservation.Reserve();
        var unusedPort = unused.Uri.Port;
        unused.Dispose();
        _ = await Assert.That(CanBind(unusedPort)).IsTrue();
    }

    /// <summary>The port number stays claimed after its socket closes, even when the allocator wraps its range.</summary>
    [Test]
    public async Task ReleasedSocketKeepsPortReserved()
    {
        using var original = ProcessPortReservation.Reserve();
        original.ReleaseForStart();

        for (var index = 0; index < 800; index++)
        {
            using var next = ProcessPortReservation.Reserve();
            _ = await Assert.That(next.Uri.Port).IsNotEqualTo(original.Uri.Port);
        }
    }

    /// <summary>Repeated configuration failures return their socket and claim instead of exhausting the range.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedConfigurationReturnsReservations(CancellationToken cancellationToken)
    {
        var failures = 0;
        for (var index = 0; index < 800; index++)
        {
            try
            {
                await using var server = await ServerProcess.StartAsync(false, cancellationToken, FailConfigurationAsync);
            }
            catch (IOException exception) when (string.Equals(exception.Message, "Configuration refused.", StringComparison.Ordinal))
            {
                failures++;
            }
        }

        _ = await Assert.That(failures).IsEqualTo(800);
    }

    private static Task FailConfigurationAsync(ProcessStartInfo start, CancellationToken cancellationToken) => Task.FromException(new IOException("Configuration refused."));

    private static bool CanBind(int port)
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.ExclusiveAddressUse = true;
            socket.Bind(new IPEndPoint(IPAddress.Loopback, port));
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
