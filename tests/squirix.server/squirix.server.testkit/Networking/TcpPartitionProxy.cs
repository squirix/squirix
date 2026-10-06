using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Networking;

/// <summary>A loopback TCP relay in front of one upstream listener that tests cut, hold, and heal on demand.</summary>
/// <remarks>
/// <para>
/// Every accepted connection is bridged to <see cref="Upstream" /> by two byte pumps, one per
/// <see cref="ProxyDirection" />. <see cref="PartitionAsync" /> resets the live connections and makes the proxy
/// reset every new one until <see cref="Heal" />; <see cref="Hold" /> parks a direction before its next write so
/// its bytes stay in the pump and kernel buffers until <see cref="Release" />, where TCP backpressure stalls the
/// sender. Nothing here is timer-driven: the counters and the wait helpers are the only observation points.
/// </para>
/// <para>
/// Teardown is bounded by closing sockets: every pending socket operation is aborted by the close, so
/// <see cref="DisposeAsync" /> and <see cref="PartitionAsync" /> never wait on a wall clock.
/// </para>
/// </remarks>
[Mutable]
public sealed class TcpPartitionProxy : IAsyncDisposable
{
    private const int BufferSize = 16 * 1024;

    private readonly Task _acceptLoop;
    private readonly HashSet<Task> _bridges = [];
    private readonly ChangeSignal _changed = new();
    private readonly DirectionGate _clientToUpstream;
    private readonly HashSet<ProxiedConnection> _connections = [];
    private readonly Counters _counters = new();
    private readonly Lock _gate = new();
    private readonly Socket _listener;
    private readonly CancellationTokenSource _stopping = new();
    private readonly DirectionGate _upstreamToClient;
    private int _disposed;
    private int _partitioned;

    private TcpPartitionProxy(IPEndPoint upstream)
    {
        Upstream = upstream;
        _clientToUpstream = new DirectionGate(_changed);
        _upstreamToClient = new DirectionGate(_changed);
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen();
            ListenEndPoint = _listener.LocalEndPoint is IPEndPoint bound ? bound : throw new InvalidOperationException("The proxy listener has no loopback endpoint.");
        }
        catch
        {
            _listener.Dispose();
            _stopping.Dispose();
            throw;
        }

        _acceptLoop = AcceptLoopAsync();
    }

    /// <summary>Gets the number of connections bridged to <see cref="Upstream" /> since start, including closed ones.</summary>
    public long AcceptedConnections => _counters.Accepted;

    /// <summary>Gets the number of connections currently bridged to <see cref="Upstream" />.</summary>
    public int ActiveConnections
    {
        get
        {
            lock (_gate)
                return _connections.Count;
        }
    }

    /// <summary>Gets a value indicating whether the proxy resets every connection instead of bridging it.</summary>
    public bool IsPartitioned => Volatile.Read(ref _partitioned) == 1;

    /// <summary>Gets the loopback endpoint clients connect to.</summary>
    public IPEndPoint ListenEndPoint { get; }

    /// <summary>Gets the number of connections reset because the proxy was partitioned; upstream connect failures are counted by <see cref="UpstreamConnectFailures" />.</summary>
    public long RefusedConnections => _counters.Refused;

    /// <summary>Gets the endpoint accepted connections are bridged to.</summary>
    public IPEndPoint Upstream { get; }

    /// <summary>Gets the number of accepted connections whose upstream connect failed.</summary>
    public long UpstreamConnectFailures => _counters.ConnectFailures;

    /// <summary>Binds an ephemeral loopback port in front of <paramref name="upstream" /> and starts accepting connections.</summary>
    /// <param name="upstream">The listener accepted connections are bridged to.</param>
    /// <param name="cancellationToken">Cancellation token for the start.</param>
    /// <returns>The started proxy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="upstream" /> is <see langword="null" />.</exception>
    public static Task<TcpPartitionProxy> StartAsync(IPEndPoint upstream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(upstream);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new TcpPartitionProxy(upstream));
    }

    /// <summary>Gets the number of bytes forwarded in <paramref name="direction" /> since start.</summary>
    /// <param name="direction">The direction to read.</param>
    /// <returns>The forwarded byte count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="direction" /> is not a named direction.</exception>
    public long BytesForwarded(ProxyDirection direction) => _counters.Forwarded(direction);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await _stopping.CancelAsync().ConfigureAwait(false);
        _listener.Dispose();
        ProxiedConnection[] connections;
        lock (_gate)
            connections = [.. _connections];

        var aborted = AbortConnections(connections);

#pragma warning disable VSTHRD003 // The accept loop is started by this proxy's constructor and ends once the listener above is closed.
        await _acceptLoop.ConfigureAwait(false);
#pragma warning restore VSTHRD003

        // The accept loop has ended, so no bridge is added after this snapshot; each one is cancelled through the stopping token.
        Task[] bridges;
        lock (_gate)
            bridges = [.. _bridges];

        await Task.WhenAll(bridges).ConfigureAwait(false);
        await Task.WhenAll(aborted).ConfigureAwait(false);
        _stopping.Dispose();
    }

    /// <summary>Bridges new connections again and releases both directions.</summary>
    public void Heal()
    {
        Volatile.Write(ref _partitioned, 0);
        _clientToUpstream.Release();
        _upstreamToClient.Release();
    }

    /// <summary>Parks the pumps of <paramref name="direction" /> before their next write until <see cref="Release" />.</summary>
    /// <param name="direction">The direction to hold.</param>
    public void Hold(ProxyDirection direction) => GateFor(direction).Hold();

    /// <summary>Gets a value indicating whether <paramref name="direction" /> is held.</summary>
    /// <param name="direction">The direction to read.</param>
    /// <returns><see langword="true" /> while the direction is held.</returns>
    public bool IsHeld(ProxyDirection direction) => GateFor(direction).IsHeld;

    /// <summary>Resets every bridged connection and makes the proxy reset new connections until <see cref="Heal" />.</summary>
    /// <returns>A task that completes once every bridged connection is gone, so <see cref="ActiveConnections" /> is zero.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the proxy has been disposed.</exception>
    public Task PartitionAsync()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        ProxiedConnection[] connections;
        lock (_gate)
        {
            Volatile.Write(ref _partitioned, 1);
            connections = [.. _connections];
        }

        return Task.WhenAll(AbortConnections(connections));
    }

    /// <summary>Lets the pumps of <paramref name="direction" /> write again.</summary>
    /// <param name="direction">The direction to release.</param>
    public void Release(ProxyDirection direction) => GateFor(direction).Release();

    /// <summary>Waits until exactly <paramref name="activeConnections" /> connections are bridged.</summary>
    /// <param name="activeConnections">The bridged connection count to wait for.</param>
    /// <param name="cancellationToken">Bounds the wait.</param>
    /// <returns>A task that completes once <see cref="ActiveConnections" /> equals the count.</returns>
    public Task WaitForActiveConnectionsAsync(int activeConnections, CancellationToken cancellationToken) =>
        _changed.WaitUntilAsync(() => ActiveConnections == activeConnections, cancellationToken);

    /// <summary>Waits until at least <paramref name="acceptedConnections" /> connections were bridged to <see cref="Upstream" />.</summary>
    /// <param name="acceptedConnections">The bridged connection count to reach.</param>
    /// <param name="cancellationToken">Bounds the wait.</param>
    /// <returns>A task that completes once the count is reached.</returns>
    public Task WaitForConnectionAsync(long acceptedConnections, CancellationToken cancellationToken) =>
        _changed.WaitUntilAsync(() => AcceptedConnections >= acceptedConnections, cancellationToken);

    /// <summary>Waits until at least <paramref name="bytes" /> bytes were forwarded in <paramref name="direction" />.</summary>
    /// <param name="direction">The direction to observe.</param>
    /// <param name="bytes">The forwarded byte count to reach.</param>
    /// <param name="cancellationToken">Bounds the wait.</param>
    /// <returns>A task that completes once the count is reached.</returns>
    public Task WaitForForwardedAsync(ProxyDirection direction, long bytes, CancellationToken cancellationToken) =>
        _changed.WaitUntilAsync(() => BytesForwarded(direction) >= bytes, cancellationToken);

    /// <summary>Waits until a pump of <paramref name="direction" /> is parked on its held gate with bytes pending.</summary>
    /// <param name="direction">The held direction.</param>
    /// <param name="cancellationToken">Bounds the wait.</param>
    /// <returns>A task that completes once a pump is parked.</returns>
    public Task WaitUntilHeldAsync(ProxyDirection direction, CancellationToken cancellationToken)
    {
        var gate = GateFor(direction);
        return _changed.WaitUntilAsync(() => gate.ParkedPumps > 0, cancellationToken);
    }

    private static Task[] AbortConnections(ProxiedConnection[] connections)
    {
        var tasks = new Task[connections.Length];
        for (var i = 0; i < connections.Length; i++)
        {
            connections[i].Abort();
            tasks[i] = connections[i].Completion;
        }

        return tasks;
    }

    private async Task AcceptLoopAsync()
    {
        var token = _stopping.Token;
        while (true)
        {
            Socket client;
            try
            {
                client = await _listener.AcceptAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (SocketException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (SocketException exception) when (exception.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionAborted)
            {
                // The client gave up before it was accepted; the next one is unaffected.
                continue;
            }

            if (IsPartitioned)
            {
                Refuse(client);
                continue;
            }

            // Each connection bridges on its own task, so a slow upstream connect never delays the next accept.
            var bridge = BridgeAsync(client, token);
            lock (_gate)
            {
                _ = _bridges.RemoveWhere(static task => task.IsCompleted);
                if (!bridge.IsCompleted)
                    _ = _bridges.Add(bridge);
            }
        }
    }

    private async Task BridgeAsync(Socket client, CancellationToken token)
    {
        Socket? pending = null;
        Socket upstream;
        try
        {
            client.NoDelay = true;
            pending = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            await pending.ConnectAsync(Upstream, token).ConfigureAwait(false);
            upstream = pending;
            pending = null;
        }
        catch (Exception exception) when (exception is SocketException or ObjectDisposedException or OperationCanceledException)
        {
            if (exception is SocketException && !token.IsCancellationRequested && pending is { Connected: false })
                _counters.CountConnectFailure();

            SocketOps.ResetAndClose(client);
            return;
        }
        finally
        {
            pending?.Dispose();
        }

        var bridged = false;
        bool partitioned;
        lock (_gate)
        {
            partitioned = IsPartitioned;
            if (!partitioned && Volatile.Read(ref _disposed) == 0)
            {
                // The pumps start under the gate so a concurrent partition snapshot always sees a started
                // connection whose completion it can await. The pumps' first socket read returns pending on
                // a live socket; a pump that ends synchronously re-enters this gate to remove itself.
                var connection = new ProxiedConnection(client, upstream);
                _ = _connections.Add(connection);
                _counters.CountAccepted();
                connection.Start(this);
                bridged = true;
            }
        }

        if (bridged)
        {
            _changed.Pulse();
            return;
        }

        SocketOps.ResetAndClose(upstream);
        if (partitioned)
            Refuse(client);
        else
            SocketOps.ResetAndClose(client);
    }

    private DirectionGate GateFor(ProxyDirection direction) => direction switch
    {
        ProxyDirection.ClientToUpstream => _clientToUpstream,
        ProxyDirection.UpstreamToClient => _upstreamToClient,
        _ => throw new ArgumentOutOfRangeException(nameof(direction), direction, "Unsupported proxy direction."),
    };

    /// <summary>Copies bytes from <paramref name="source" /> to <paramref name="destination" /> until a FIN or an abort; closing a socket is what aborts the pump, so no token is taken.</summary>
    /// <param name="connection">The connection the pump belongs to.</param>
    /// <param name="source">The socket read from.</param>
    /// <param name="destination">The socket written to.</param>
    /// <param name="direction">The direction the pump carries, selecting its gate and counter.</param>
    private async Task PumpAsync(ProxiedConnection connection, Socket source, Socket destination, ProxyDirection direction)
    {
        var gate = GateFor(direction);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            while (true)
            {
                var read = await source.ReceiveAsync(buffer.AsMemory(0, BufferSize), SocketFlags.None, CancellationToken.None).ConfigureAwait(false);
                if (read == 0)
                {
                    destination.Shutdown(SocketShutdown.Send);
                    return;
                }

                if (!await gate.WaitOpenAsync(connection).ConfigureAwait(false))
                    return;

                await SocketOps.SendAllAsync(destination, buffer.AsMemory(0, read)).ConfigureAwait(false);
                _counters.AddForwarded(direction, read);
                _changed.Pulse();
            }
        }
        catch (Exception exception) when (exception is SocketException or ObjectDisposedException)
        {
            // Either side failed or was closed by an abort; take the other side down with it.
            connection.Abort();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void Refuse(Socket client)
    {
        _counters.CountRefused();
        SocketOps.ResetAndClose(client);
        _changed.Pulse();
    }

    private void Remove(ProxiedConnection connection)
    {
        lock (_gate)
            _ = _connections.Remove(connection);

        _changed.Pulse();
    }

    /// <summary>Socket teardown and write helpers shared by the accept loop, the pumps, and the connections.</summary>
    private static class SocketOps
    {
        /// <summary>Closes a socket with a reset instead of a graceful FIN; a socket the peer already reset fails the linger change and is simply disposed.</summary>
        /// <param name="socket">The socket to reset.</param>
        internal static void ResetAndClose(Socket socket)
        {
            try
            {
                socket.LingerState = new LingerOption(true, 0);
            }
            catch (Exception exception) when (exception is SocketException or ObjectDisposedException)
            {
                // Nothing to linger on: the peer is gone, and disposing below is all that is left.
            }

            socket.Dispose();
        }

        /// <summary>Sends every byte of <paramref name="data" />; closing the socket is the only way to abort it, so no token is taken.</summary>
        /// <param name="destination">The socket to write to.</param>
        /// <param name="data">The bytes to send.</param>
        internal static async ValueTask SendAllAsync(Socket destination, ReadOnlyMemory<byte> data)
        {
            while (!data.IsEmpty)
            {
                var sent = await destination.SendAsync(data, SocketFlags.None, CancellationToken.None).ConfigureAwait(false);
                data = data[sent..];
            }
        }
    }

    /// <summary>A completion swapped on every pulse, so a waiter that captures it before checking its condition cannot miss an update.</summary>
    [Mutable]
    private sealed class ChangeSignal
    {
        private readonly Lock _lock = new();
        private TaskCompletionSource _next = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the completion of the next pulse.</summary>
        internal Task Next
        {
            get
            {
#pragma warning disable VSTHRD003 // The task is a completion source owned and pulsed by this signal.
                lock (_lock)
                    return _next.Task;
#pragma warning restore VSTHRD003
            }
        }

        /// <summary>Waits until <paramref name="condition" /> holds, re-checking it after every pulse.</summary>
        /// <param name="condition">The condition to wait for.</param>
        /// <param name="cancellationToken">Bounds the wait.</param>
        /// <returns>A task that completes once the condition holds.</returns>
        internal async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
        {
            while (true)
            {
                var next = Next;
                if (condition())
                    return;

                await next.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal void Pulse()
        {
            TaskCompletionSource completed;
            lock (_lock)
            {
                completed = _next;
                _next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            completed.SetResult();
        }
    }

    /// <summary>The proxy's observable counters.</summary>
    [Mutable]
    private sealed class Counters
    {
        private long _accepted;
        private long _clientToUpstreamBytes;
        private long _connectFailures;
        private long _refused;
        private long _upstreamToClientBytes;

        internal long Accepted => Interlocked.Read(ref _accepted);

        internal long ConnectFailures => Interlocked.Read(ref _connectFailures);

        internal long Refused => Interlocked.Read(ref _refused);

        internal void AddForwarded(ProxyDirection direction, int bytes)
        {
            if (direction == ProxyDirection.ClientToUpstream)
                _ = Interlocked.Add(ref _clientToUpstreamBytes, bytes);
            else
                _ = Interlocked.Add(ref _upstreamToClientBytes, bytes);
        }

        internal void CountAccepted() => _ = Interlocked.Increment(ref _accepted);

        internal void CountConnectFailure() => _ = Interlocked.Increment(ref _connectFailures);

        internal void CountRefused() => _ = Interlocked.Increment(ref _refused);

        internal long Forwarded(ProxyDirection direction) => direction switch
        {
            ProxyDirection.ClientToUpstream => Interlocked.Read(ref _clientToUpstreamBytes),
            ProxyDirection.UpstreamToClient => Interlocked.Read(ref _upstreamToClientBytes),
            _ => throw new ArgumentOutOfRangeException(nameof(direction), direction, "Unsupported proxy direction."),
        };
    }

    /// <summary>A per-direction gate the pumps await before each write.</summary>
    [Mutable]
    private sealed class DirectionGate
    {
        private readonly ChangeSignal _changed;
        private readonly Lock _lock = new();
        private bool _held;
        private int _parkedPumps;
        private TaskCompletionSource? _released;

        internal DirectionGate(ChangeSignal changed)
        {
            _changed = changed;
        }

        internal bool IsHeld
        {
            get
            {
                lock (_lock)
                    return _held;
            }
        }

        internal int ParkedPumps => Volatile.Read(ref _parkedPumps);

        internal void Hold()
        {
            lock (_lock)
            {
                if (_held)
                    return;

                _held = true;
                _released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        internal void Release()
        {
            TaskCompletionSource? released;
            lock (_lock)
            {
                if (!_held)
                    return;

                _held = false;
                released = _released;
                _released = null;
            }

            released?.SetResult();
        }

        /// <summary>Waits until the gate is open or <paramref name="connection" /> is aborted.</summary>
        /// <param name="connection">The connection whose pump waits.</param>
        /// <returns><see langword="true" /> when the gate opened; <see langword="false" /> when the connection was aborted first.</returns>
        internal async ValueTask<bool> WaitOpenAsync(ProxiedConnection connection)
        {
            Task open;
            lock (_lock)
            {
                if (!_held)
                    return true;

                open = _released!.Task;
            }

            _ = Interlocked.Increment(ref _parkedPumps);
            _changed.Pulse();
            try
            {
#pragma warning disable VSTHRD003 // Both tasks are completion sources owned by this proxy: the gate's release and the connection's abort.
                var first = await Task.WhenAny(open, connection.Aborted).ConfigureAwait(false);
#pragma warning restore VSTHRD003
                return first == open;
            }
            finally
            {
                _ = Interlocked.Decrement(ref _parkedPumps);
                _changed.Pulse();
            }
        }
    }

    /// <summary>One bridged connection: the client socket, its upstream socket, and the two pumps between them.</summary>
    [Mutable]
    private sealed class ProxiedConnection
    {
        private readonly TaskCompletionSource _aborted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Socket _client;
        private readonly Socket _upstream;
        private int _closed;

        internal ProxiedConnection(Socket client, Socket upstream)
        {
            _client = client;
            _upstream = upstream;
        }

        /// <summary>Gets a task that completes once the connection is aborted; pumps parked on a held gate race it against the gate.</summary>
        internal Task Aborted => _aborted.Task;

        /// <summary>Gets a task that completes once both pumps ended and the connection left its proxy.</summary>
        internal Task Completion { get; private set; } = Task.CompletedTask;

        /// <summary>Resets both sockets, which aborts every pending socket operation of the pumps; later calls are no-ops.</summary>
        internal void Abort()
        {
            if (Interlocked.Exchange(ref _closed, 1) == 1)
                return;

            _aborted.SetResult();
            SocketOps.ResetAndClose(_client);
            SocketOps.ResetAndClose(_upstream);
        }

        internal void Start(TcpPartitionProxy owner) => Completion = RunAsync(owner);

        private async Task RunAsync(TcpPartitionProxy owner)
        {
            try
            {
                var toUpstream = owner.PumpAsync(this, _client, _upstream, ProxyDirection.ClientToUpstream);
                var toClient = owner.PumpAsync(this, _upstream, _client, ProxyDirection.UpstreamToClient);
                await Task.WhenAll(toUpstream, toClient).ConfigureAwait(false);
            }
            finally
            {
                // Both pumps ended on their own (FIN both ways): close gracefully unless an abort already reset the sockets.
                if (Interlocked.Exchange(ref _closed, 1) == 0)
                {
                    _client.Dispose();
                    _upstream.Dispose();
                }

                owner.Remove(this);
            }
        }
    }
}
