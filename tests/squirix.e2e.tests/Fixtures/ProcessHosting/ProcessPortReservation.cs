using System;
using System.Threading;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Fixtures.ProcessHosting;

/// <summary>Reserves a smoke-process port until shutdown, holding its socket until process startup.</summary>
internal sealed class ProcessPortReservation : IDisposable
{
    private HeldPort? _port;

    private ProcessPortReservation(HeldPort port)
    {
        _port = port;
        Uri = new Uri($"https://localhost:{port.Port}");
    }

    /// <summary>Gets the reserved server endpoint.</summary>
    internal Uri Uri { get; }

    /// <inheritdoc />
    public void Dispose() => Interlocked.Exchange(ref _port, null)?.Return();

    /// <summary>Claims a port from this process's exclusive smoke-test slice and holds it bound while configuration is prepared.</summary>
    /// <returns>The reservation, which must outlive the server process.</returns>
    internal static ProcessPortReservation Reserve() => new(ListenPortPool.SmokeTests.HoldPort());

    /// <summary>Closes the socket immediately before starting the child, retaining the in-process reservation and process-wide slice claim.</summary>
    internal void ReleaseForStart() => _port?.Dispose();
}
