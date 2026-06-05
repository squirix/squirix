using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Allocates loopback ports from a deterministic process-local range.
/// </summary>
internal static class E2EBenchmarkPortAllocator
{
    private static int _next = 20_000 + (Environment.ProcessId % 10_000);

    internal static string NextHttpUrl() => $"http://127.0.0.1:{NextPort()}";

    private static int NextPort()
    {
        for (var attempt = 0; attempt < 512; attempt++)
        {
            var candidate = Interlocked.Increment(ref _next);
            if (candidate > 60_000)
            {
                _ = Interlocked.Exchange(ref _next, 20_000);
                candidate = Interlocked.Increment(ref _next);
            }

            if (CanBind(candidate))
                return candidate;
        }

        throw new InvalidOperationException("Unable to allocate a local benchmark port.");
    }

    private static bool CanBind(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
