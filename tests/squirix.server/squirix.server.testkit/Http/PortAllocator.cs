using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Squirix.Server.TestKit.Http;

/// <summary>
/// Port allocator with process-wide synchronization and bind probes to reduce collisions.
/// Note: still TOCTOU across processes; use different ranges per process to avoid conflicts.
/// </summary>
public sealed class PortAllocator
{
    // Process-wide reservation to avoid duplicates between allocators inside one process
    private static readonly ConcurrentDictionary<int, byte> Reserved = new();
    private readonly int _endInclusive;
    private readonly int _rangeSize;
    private readonly int _start;
    private int _next; // rolling cursor

    /// <summary>
    /// Initializes a new instance of the <see cref="PortAllocator" /> class.
    /// </summary>
    /// <param name="startPort">Inclusive lower bound of the port range (1–65535).</param>
    /// <param name="endPortInclusive">Inclusive upper bound of the port range (1–65535).</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if either <paramref name="startPort" /> or <paramref name="endPortInclusive" /> is outside 1–65535.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="endPortInclusive" /> is less than <paramref name="startPort" />.
    /// </exception>
    /// <remarks>
    /// The allocator will hand out ports within <c>[startPort, endPortInclusive]</c> on subsequent allocation calls.
    /// This constructor only validates numeric bounds; it does not probe the OS for port availability.
    /// </remarks>
    public PortAllocator(int startPort, int endPortInclusive)
    {
        if (startPort is <= 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(startPort));
        if (endPortInclusive is <= 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(endPortInclusive));
        if (endPortInclusive < startPort)
            throw new ArgumentException("endPortInclusive must be >= startPort");

        _start = startPort;
        _endInclusive = endPortInclusive;
        _rangeSize = _endInclusive - _start + 1;
        _next = _start - 1;
    }

    /// <summary>
    /// Allocates a currently free port within the allocator’s configured inclusive range.
    /// </summary>
    /// <param name="maxAttempts">
    /// The maximum number of candidate ports to try before giving up. Higher values increase the
    /// likelihood of finding a free port at the cost of additional probing. The default is 3,000.
    /// </param>
    /// <returns>The port number that appeared free at the time of probing.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no free port can be reserved within the attempt budget.
    /// </exception>
    /// <remarks>
    ///     <para>
    ///     The method first reserves the candidate port within the current process (to avoid duplicate
    ///     selection by concurrent callers), then probes the OS by binding and immediately releasing the
    ///     port. If probing succeeds, the port is returned; otherwise the in-process reservation is removed
    ///     and the next candidate is tried.
    ///     </para>
    ///     <para>
    ///     This call reduces—but cannot eliminate—TOCTOU races with other processes. Use the returned port
    ///     immediately to perform your real bind. The in-process reservation only prevents duplicates within
    ///     this process; it is not a system-wide reservation.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// var port = allocator.Allocate();
    /// using var listener = new TcpListener(IPAddress.Loopback, port);
    /// listener.Start();
    /// </code>
    /// </example>
    public int Allocate(int maxAttempts = 3_000)
    {
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var candidate = NextCandidate();

            // Reserve within-process first to avoid duplicates
            if (!Reserved.TryAdd(candidate, 0))
                continue;

            if (ProbeBind(candidate))
            {
                // Port appears free (bind succeeded and released)
                return candidate;
            }

            // Release reservation on failure and continue
            _ = Reserved.TryRemove(candidate, out _);
        }

        throw new InvalidOperationException($"Failed to allocate a free port in range {_start}-{_endInclusive} after {maxAttempts} attempts.");
    }

    private static bool ProbeBind(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Server.ExclusiveAddressUse = true;
            listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
            listener.Start();

            // If Start() succeeds, port is bindable -> release immediately
            listener.Stop();
            return true;
        }
        catch
        {
            return false;
        }
    }

    private int NextCandidate()
    {
        var cur = Interlocked.Increment(ref _next);
        var offset = (cur - _start) % _rangeSize;
        if (offset < 0)
            offset += _rangeSize;
        return _start + offset;
    }
}
