using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Squirix.Server.TestKit.Networking;

/// <summary>
/// Port allocator with process-wide synchronization and bind probes to reduce collisions.
/// Note: still TOCTOU across processes; use different ranges per process to avoid conflicts.
/// </summary>
public sealed class PortAllocator : IDisposable
{
    /// <summary>Process-wide reservation to avoid duplicates between allocators inside one process.</summary>
    private static readonly ConcurrentDictionary<int, byte> Reserved = new();

    private readonly ConcurrentBag<int> _allocatedPorts = [];
    private readonly ConcurrentDictionary<int, TcpListener> _heldPorts = new();
    private readonly int _rangeSize;
    private readonly int _start;
    private int _disposed;

    /// <summary>Rolling cursor.</summary>
    private int _next;

    /// <summary>
    /// Initializes a new instance of the <see cref="PortAllocator" /> class.
    /// </summary>
    /// <param name="startPort">Inclusive lower bound of the port range (1–65,535).</param>
    /// <param name="endPortInclusive">Inclusive upper bound of the port range (1–65,535).</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if either <paramref name="startPort" /> or <paramref name="endPortInclusive" /> is outside 1–65,535.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="endPortInclusive" /> is less than <paramref name="startPort" />.
    /// </exception>
    /// <remarks>
    /// The allocator will hand out ports within <c language="csharp">[startPort, endPortInclusive]</c> on later allocation calls.
    /// This constructor only validates numeric bounds; it does not probe the OS for port availability.
    /// </remarks>
    public PortAllocator(int startPort, int endPortInclusive)
    {
        if (startPort is <= 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(startPort));
        if (endPortInclusive is <= 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(endPortInclusive));
        if (endPortInclusive < startPort)
            throw new ArgumentException("endPortInclusive must be >= startPort", nameof(endPortInclusive));

        _start = startPort;
        var endInclusive = endPortInclusive;
        _rangeSize = endInclusive - _start + 1;
        _next = _start + (CreateProcessOffset() % _rangeSize) - 1;
    }

    /// <summary>Allocates a currently free port within the allocator’s configured inclusive range.</summary>
    /// <param name="maxAttempts">
    /// The maximum number of candidate ports to try before giving up. Higher values increase the
    /// likelihood of finding a free port at the cost of additional probing. The default is 3,000.
    /// </param>
    /// <returns>The port number that appeared free at the time of probing.</returns>
    /// <exception cref="InvalidOperationException">Thrown if no free port can be reserved within the attempt budget.</exception>
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
    ///     <code language="csharp">
    /// var port = allocator.Allocate();
    /// using var listener = new TcpListener(IPAddress.Loopback, port);
    /// listener.Start();
    /// </code>
    /// </example>
    public int Allocate(int maxAttempts = 3_000)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var candidate = NextCandidate();

            // Reserve within-process first to avoid duplicates
            if (!Reserved.TryAdd(candidate, 0))
                continue;

            if (ProbeBind(candidate))
            {
                // Port appears free (bind succeeded and released)
                _allocatedPorts.Add(candidate);
                return candidate;
            }

            // Release reservation on failure and continue
            _ = Reserved.TryRemove(candidate, out _);
        }

        throw new InvalidOperationException("Failed to allocate a free listen port.");
    }

    /// <summary>Releases a previously reserved port so the actual server can bind to it.</summary>
    /// <param name="port">The port number to release.</param>
    /// <remarks>
    /// The port is unbound, and the caller should bind it immediately to minimize the TOCTOU window.
    /// The port stays reserved in-process until the allocator is disposed, so the pool will not hand it
    /// out again to a later caller.
    /// </remarks>
    public void ReleasePort(int port)
    {
        if (!_heldPorts.TryRemove(port, out var listener))
            return;
        listener.Stop();
        listener.Dispose();
    }

    /// <summary>
    /// Reserves a contiguous range of <paramref name="count" /> free ports and holds them all bound
    /// simultaneously so the pool does not hand the same port to overlapping callers.
    /// </summary>
    /// <param name="count">Number of consecutive free ports to reserve.</param>
    /// <param name="maxAttempts">The maximum number of candidate starting ports to try before giving up. The default is 3,000.</param>
    /// <returns>The reserved port numbers, all bound and held open until released via <see cref="ReleasePort" />.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="count" /> is less than 1.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no contiguous range of <paramref name="count" /> free ports can be found within the attempt budget.
    /// </exception>
    /// <remarks>
    /// Each port in the returned range stays bound (with exclusive address use) and marked as an in-process
    /// reservation until the caller releases it via <see cref="ReleasePort" />, so the pool will not hand any of
    /// these ports to another caller. A released port stays reserved in-process; the caller should bind it
    /// quickly, because an unrelated third-party process could still grab it in the brief gap before the real bind.
    /// </remarks>
    public int[] ReserveRange(int count, int maxAttempts = 3_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var candidate = NextCandidate();
            if (!FitsInRange(candidate, count))
                continue;

            var ports = new int[count];
            if (TryReserve(ResolveWithinRange(candidate), ports))
                return ports;
        }

        throw new InvalidOperationException($"Failed to reserve a contiguous range of {count} free listen ports.");
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var (port, listener) in _heldPorts)
        {
            listener.Stop();
            listener.Dispose();
            _ = Reserved.TryRemove(port, out _);
        }

        _heldPorts.Clear();

        foreach (var port in _allocatedPorts)
            _ = Reserved.TryRemove(port, out _);

        _allocatedPorts.Clear();
    }

    private static TcpListener BindPort(int port)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        try
        {
            listener.Server.ExclusiveAddressUse = true;
            listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
            listener.Start();
            return listener;
        }
        catch
        {
            listener.Dispose();
            throw;
        }
    }

    private static int CreateProcessOffset()
    {
        unchecked
        {
            var hash = Environment.ProcessId;
            foreach (var ch in AppContext.BaseDirectory)
                hash = (hash * 31) + ch;

            return hash & int.MaxValue;
        }
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
        catch (SocketException)
        {
            return false;
        }
    }

    private bool FitsInRange(int candidate, int count)
    {
        var offset = (candidate - _start) % _rangeSize;
        if (offset < 0)
            offset += _rangeSize;
        return offset + count <= _rangeSize;
    }

    private int NextCandidate()
    {
        var cur = Interlocked.Increment(ref _next);
        var offset = (cur - _start) % _rangeSize;
        if (offset < 0)
            offset += _rangeSize;
        return _start + offset;
    }

    private int ResolveWithinRange(int candidate)
    {
        var offset = (candidate - _start) % _rangeSize;
        if (offset < 0)
            offset += _rangeSize;
        return _start + offset;
    }

    private bool TryReserve(int start, int[] ports)
    {
        var reservedCount = 0;
        var listeners = new TcpListener[ports.Length];
        try
        {
            for (var i = 0; i < ports.Length; i++)
            {
                var port = start + i;
                if (!Reserved.TryAdd(port, 0))
                    return false;

                try
                {
                    listeners[i] = BindPort(port);
                    ports[i] = port;
                    reservedCount++;
                    _heldPorts[port] = listeners[i];
                    _allocatedPorts.Add(port);
                }
                catch (SocketException)
                {
                    // The port was reserved but could not be bound; drop its reservation before failing.
                    _ = Reserved.TryRemove(port, out _);
                    return false;
                }
            }

            return true;
        }
        finally
        {
            if (reservedCount < ports.Length)
            {
                for (var i = reservedCount - 1; i >= 0; i--)
                {
                    var port = ports[i];
                    _ = _heldPorts.TryRemove(port, out var listener);
                    listener?.Stop();
                    listener?.Dispose();
                    _ = Reserved.TryRemove(port, out _);
                }
            }
        }
    }
}
