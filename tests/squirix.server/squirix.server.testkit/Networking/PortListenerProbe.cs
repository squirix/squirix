using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Checks that nothing listens on a loopback port any more, and names what still holds it when something does.</summary>
/// <remarks>
/// On BSD/macOS a listening socket that was just closed can keep its port from a new bind for a while, so a bind does not tell whether a
/// listener remains; a connection does: it is refused at once when nothing listens and accepted into the backlog when something does. On
/// Windows a refused loopback connection is only reported after the SYN retries, so the check binds the port exclusively there instead,
/// which fails only while a socket still holds it.
/// </remarks>
public static class PortListenerProbe
{
    /// <summary>Bounds the connection attempt and the socket listing taken on failure.</summary>
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Asserts that nothing listens on <paramref name="port" /> of the IPv4 loopback address.</summary>
    /// <param name="port">The port.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the port has no listener.</returns>
    /// <exception cref="InvalidOperationException">Something still listens on the port; the message lists the sockets that use it.</exception>
    public static async Task AssertNoListenerAsync(int port, CancellationToken cancellationToken)
    {
        var failure = OperatingSystem.IsWindows() ? ExclusiveBindFailure(port) : await ConnectionFailureAsync(port, cancellationToken).ConfigureAwait(false);
        if (failure == null)
            return;

        var sockets = await DescribeSocketsAsync(port, cancellationToken).ConfigureAwait(false);
        throw new InvalidOperationException(
            string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{port} still has a listener: {failure}.{Environment.NewLine}{sockets}"));
    }

    /// <summary>Checks the connection result, retrying resets within one shared deadline.</summary>
    /// <param name="port">The port.</param>
    /// <param name="connect">The socket connection operation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The failure, or <see langword="null" /> only after a connection refusal.</returns>
    internal static async Task<string?> ProbeConnectionAsync(int port, Func<Socket, EndPoint, CancellationToken, ValueTask> connect, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ProbeTimeout);
        try
        {
            while (true)
            {
                using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    await connect(probe, new IPEndPoint(IPAddress.Loopback, port), timeout.Token).ConfigureAwait(false);
                    return "a connection was accepted";
                }
                catch (SocketException exception) when (exception.SocketErrorCode == SocketError.ConnectionRefused)
                {
                    return null;
                }
                catch (SocketException exception) when (exception.SocketErrorCode == SocketError.ConnectionReset)
                {
                    // A closing macOS socket can reset a connection before subsequent attempts are refused.
                    // A reset alone does not prove that the listener is gone.
                    await Task.Delay(TimeSpan.FromMilliseconds(25), TimeProvider.System, timeout.Token).ConfigureAwait(false);
                }
                catch (SocketException exception)
                {
                    return $"a connection failed with {exception.SocketErrorCode} instead of being refused";
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return "a connection was neither accepted nor refused within the probe timeout";
        }
    }

    /// <summary>Binds the port exclusively and releases it.</summary>
    /// <param name="port">The port.</param>
    /// <returns>Why the bind failed, or <see langword="null" /> when it succeeded.</returns>
    private static string? ExclusiveBindFailure(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        try
        {
            listener.Start();
            listener.Stop();
            return null;
        }
        catch (SocketException exception)
        {
            return $"an exclusive bind failed with {exception.SocketErrorCode}";
        }
    }

    /// <summary>Connects to the port.</summary>
    /// <param name="port">The port.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What happened instead of a refusal, or <see langword="null" /> when the connection was refused.</returns>
    private static Task<string?> ConnectionFailureAsync(int port, CancellationToken cancellationToken) =>
        ProbeConnectionAsync(port, static (socket, endpoint, token) => socket.ConnectAsync(endpoint, token), cancellationToken);

    /// <summary>Lists the sockets that use the port with the system tool, so a failure shows which socket holds it and in which state.</summary>
    /// <param name="port">The port.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The listing, or why it could not be taken.</returns>
    private static async Task<string> DescribeSocketsAsync(int port, CancellationToken cancellationToken)
    {
        var windows = OperatingSystem.IsWindows();
        var start = new ProcessStartInfo(windows ? "netstat" : "lsof", windows ? "-ano -p tcp" : string.Create(CultureInfo.InvariantCulture, $"-nP -iTCP:{port}"))
        {
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };

        Process? process = null;
        try
        {
            process = Process.Start(start);
            if (process == null)
                return "Sockets on the port: the listing tool did not start.";

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(ProbeTimeout);
            var output = await process.StandardOutput.ReadToEndAsync(timeout.Token).ConfigureAwait(false);
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            return "Sockets on the port:" + Environment.NewLine + (windows ? LinesNaming(output, port) : output);
        }
        catch (Exception exception) when (exception is Win32Exception or InvalidOperationException)
        {
            return "Sockets on the port could not be listed: " + exception.Message;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return "Sockets on the port could not be listed within the probe timeout.";
        }
        finally
        {
            StopListing(process);
        }
    }

    /// <summary>Kills a listing tool that is still running, so a hung tool does not outlive the test, and disposes it.</summary>
    /// <param name="process">The tool, or <see langword="null" /> when it did not start.</param>
    private static void StopListing(Process? process)
    {
        if (process == null)
            return;

        try
        {
            if (!process.HasExited)
                process.Kill(true);
        }
        catch (InvalidOperationException)
        {
            // The tool exited between the check and the kill.
        }
        finally
        {
            process.Dispose();
        }
    }

    private static string LinesNaming(string output, int port)
    {
        var suffix = string.Create(CultureInfo.InvariantCulture, $":{port} ");
        var lines = new StringBuilder();
        foreach (var line in output.AsSpan().EnumerateLines())
        {
            if (line.Contains(suffix, StringComparison.Ordinal))
                _ = lines.Append(line).AppendLine();
        }

        return lines.ToString();
    }
}
