using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Microsoft.Win32.SafeHandles;

namespace Squirix.E2ETests.Fixtures.ProcessHosting;

/// <summary>Reserves a smoke-process port until shutdown, holding its socket until process startup.</summary>
internal sealed class ProcessPortReservation : IDisposable
{
    /// <summary>Starts the smoke-only region above shared test-host ports and below Linux's default ephemeral range.</summary>
    private const int FirstPort = 32_000;
    private const int PortCount = 768;
    private static int _next = Environment.ProcessId % PortCount;
    private TcpListener? _listener;
    private SafeFileHandle? _claim;

    private ProcessPortReservation(TcpListener listener, SafeFileHandle claim, int port)
    {
        _listener = listener;
        _claim = claim;
        Uri = new Uri($"https://localhost:{port}");
    }

    /// <summary>Gets the reserved server endpoint.</summary>
    internal Uri Uri { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Exchange(ref _listener, null)?.Dispose();
        Interlocked.Exchange(ref _claim, null)?.Dispose();
    }

    /// <summary>Claims a port across test processes and holds it bound while configuration is prepared.</summary>
    /// <returns>The reservation, which must outlive the server process.</returns>
    /// <exception cref="InvalidOperationException">No free port could be claimed in the smoke-only region.</exception>
    internal static ProcessPortReservation Reserve()
    {
        var directory = Path.Join(Path.GetTempPath(), "squirix-process-smoke-ports");
        _ = Directory.CreateDirectory(directory);
        for (var attempt = 0; attempt < PortCount; attempt++)
        {
            var offset = Interlocked.Increment(ref _next) % PortCount;
            if (offset < 0)
                offset += PortCount;

            var port = FirstPort + offset;
            SafeFileHandle? claim = null;
            TcpListener? listener = null;
            try
            {
                // Keep the lock file: deleting it would let another process lock a different file at the same path.
                claim = File.OpenHandle(Path.Join(directory, $"{port}.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                listener = new TcpListener(IPAddress.Loopback, port);
                listener.Server.ExclusiveAddressUse = true;
                listener.Start();
                var reservation = new ProcessPortReservation(listener, claim, port);
                listener = null;
                claim = null;
                return reservation;
            }
            catch (Exception exception) when (exception is IOException or SocketException)
            {
                // Another process holds the claim or has already bound this port.
            }
            finally
            {
                listener?.Dispose();
                claim?.Dispose();
            }
        }

        throw new InvalidOperationException("No smoke-process listen port could be reserved.");
    }

    /// <summary>Closes the socket immediately before starting the child, retaining the cross-process claim.</summary>
    internal void ReleaseForStart()
    {
        var listener = Interlocked.Exchange(ref _listener, null);
        if (listener == null)
            return;

        listener.Dispose();
        var started = Stopwatch.GetTimestamp();
        while (!CanBind(Uri.Port) && Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(5))
            _ = SpinWait.SpinUntil(static () => false, 10);
    }

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
