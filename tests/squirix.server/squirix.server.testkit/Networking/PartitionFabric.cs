using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Networking;

/// <summary>One <see cref="TcpPartitionProxy" /> per directed node pair, so tests cut and heal the links of a cluster by node identifier.</summary>
/// <remarks>
/// The proxy keyed <c language="csharp">(from, to)</c> carries the connections <c language="csharp">from</c> dials
/// towards <c language="csharp">to</c>. Dispose the fabric after the cluster that dials through it, so node shutdown
/// still finds its proxies. A black-holed node is never dialed: its dials hang until their connect is canceled, as
/// towards a host that is down and drops connection attempts instead of refusing them.
/// </remarks>
[Mutable]
public sealed class PartitionFabric : IAsyncDisposable
{
    private readonly HashSet<string> _blackHoled = [with(StringComparer.Ordinal)];
    private readonly Lock _gate = new();
    private readonly Dictionary<ProxyKey, TcpPartitionProxy> _proxies = [];
    private int _disposed;

    /// <summary>Gets the proxy carrying the connections <paramref name="from" /> dials towards <paramref name="to" />.</summary>
    /// <param name="from">The dialing node identifier.</param>
    /// <param name="to">The dialed node identifier.</param>
    /// <exception cref="KeyNotFoundException">Thrown when no proxy was created for the pair.</exception>
    public TcpPartitionProxy this[string from, string to]
    {
        get
        {
            lock (_gate)
            {
                return _proxies.TryGetValue(new ProxyKey(from, to), out var proxy)
                    ? proxy
                    : throw new KeyNotFoundException($"No partition proxy carries the link {from} -> {to}.");
            }
        }
    }

    /// <summary>Creates a socket connect callback that dials the proxy for the pair instead of the upstream endpoint.</summary>
    /// <param name="from">The dialing node identifier.</param>
    /// <param name="to">The dialed node identifier.</param>
    /// <returns>A callback for <see cref="SocketsHttpHandler.ConnectCallback" />; the proxy is resolved on every connect.</returns>
    public Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> CreateConnectCallback(string from, string to)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);
        return (_, cancellationToken) => IsBlackHoled(to) ? HangAsync(cancellationToken) : ConnectAsync(this[from, to].ListenEndPoint, cancellationToken);
    }

    /// <summary>Makes every new dial towards <paramref name="nodeId" /> hang until its connect is canceled, and resets the connections bridged to it.</summary>
    /// <param name="nodeId">The node whose host stops answering.</param>
    /// <returns>A task that completes once no connection towards the node is bridged; <see cref="HealAll" /> lifts the black hole.</returns>
    /// <remarks>A dial that hangs never reaches a proxy, so no proxy counts it; the connect timeout of the dialing handler is what ends it.</remarks>
    public Task BlackHoleAsync(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var partitions = new List<Task>();
        lock (_gate)
        {
            _ = _blackHoled.Add(nodeId);
            foreach (var (key, proxy) in _proxies)
            {
                if (string.Equals(key.To, nodeId, StringComparison.Ordinal))
                    partitions.Add(proxy.PartitionAsync());
            }
        }

        return Task.WhenAll(partitions);
    }

    /// <summary>
    /// Makes every new dial towards <paramref name="nodeId" /> hang until its connect is canceled, and holds both directions of the connections
    /// already bridged to it: they stay open and silent, as towards a host that went down without resetting them.
    /// </summary>
    /// <param name="nodeId">The node whose host stops answering.</param>
    /// <remarks><see cref="HealAll" /> lifts the black hole and releases what was held.</remarks>
    public void BlackHoleSilently(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        lock (_gate)
        {
            _ = _blackHoled.Add(nodeId);
            foreach (var (key, proxy) in _proxies)
            {
                if (!string.Equals(key.To, nodeId, StringComparison.Ordinal))
                    continue;

                proxy.Hold(ProxyDirection.ClientToUpstream);
                proxy.Hold(ProxyDirection.UpstreamToClient);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        TcpPartitionProxy[] proxies;
        lock (_gate)
        {
            proxies = [.. _proxies.Values];
            _proxies.Clear();
        }

        // Every proxy is disposed even when another one fails; the combined task then rethrows a failure.
        var disposals = new Task[proxies.Length];
        for (var i = 0; i < proxies.Length; i++)
            disposals[i] = proxies[i].DisposeAsync().AsTask();

        await Task.WhenAll(disposals).ConfigureAwait(false);
    }

    /// <summary>Returns the proxy for the pair, starting one in front of <paramref name="upstream" /> when the pair is new.</summary>
    /// <param name="from">The dialing node identifier.</param>
    /// <param name="to">The dialed node identifier.</param>
    /// <param name="upstream">The endpoint <paramref name="to" /> listens on for <paramref name="from" />.</param>
    /// <param name="cancellationToken">Cancellation token for the start.</param>
    /// <returns>The proxy carrying the link.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the pair already has a proxy in front of another endpoint.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the fabric has been disposed.</exception>
    public Task<TcpPartitionProxy> EnsureProxyAsync(string from, string to, IPEndPoint upstream, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);
        ArgumentNullException.ThrowIfNull(upstream);

        var key = new ProxyKey(from, to);
        return TryGet(key, upstream, out var existing) ? Task.FromResult(existing) : StartProxyAsync(key, upstream, cancellationToken);
    }

    /// <summary>Heals both links between <paramref name="a" /> and <paramref name="b" />.</summary>
    /// <param name="a">One node identifier.</param>
    /// <param name="b">The other node identifier.</param>
    public void Heal(string a, string b)
    {
        this[a, b].Heal();
        this[b, a].Heal();
    }

    /// <summary>Heals every link of the fabric and lifts every black hole.</summary>
    public void HealAll()
    {
        lock (_gate)
            _blackHoled.Clear();

        foreach (var proxy in Snapshot())
            proxy.Heal();
    }

    /// <summary>Holds the bytes <paramref name="from" /> sends to <paramref name="to" /> on both links, whichever side dialed.</summary>
    /// <param name="from">The sending node identifier.</param>
    /// <param name="to">The receiving node identifier.</param>
    public void HoldDirection(string from, string to)
    {
        this[from, to].Hold(ProxyDirection.ClientToUpstream);
        this[to, from].Hold(ProxyDirection.UpstreamToClient);
    }

    /// <summary>Partitions every link that <paramref name="nodeId" /> dials or is dialed on.</summary>
    /// <param name="nodeId">The node to isolate.</param>
    /// <returns>A task that completes once every affected proxy has no bridged connection left.</returns>
    public Task IsolateAsync(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var partitions = new List<Task>();
        lock (_gate)
        {
            foreach (var (key, proxy) in _proxies)
            {
                if (key.Touches(nodeId))
                    partitions.Add(proxy.PartitionAsync());
            }
        }

        return Task.WhenAll(partitions);
    }

    /// <summary>Partitions both links between <paramref name="a" /> and <paramref name="b" />.</summary>
    /// <param name="a">One node identifier.</param>
    /// <param name="b">The other node identifier.</param>
    /// <returns>A task that completes once both proxies have no bridged connection left.</returns>
    public Task PartitionAsync(string a, string b) => Task.WhenAll(this[a, b].PartitionAsync(), this[b, a].PartitionAsync());

    /// <summary>Releases the bytes <paramref name="from" /> sends to <paramref name="to" /> on both links.</summary>
    /// <param name="from">The sending node identifier.</param>
    /// <param name="to">The receiving node identifier.</param>
    public void ReleaseDirection(string from, string to)
    {
        this[from, to].Release(ProxyDirection.ClientToUpstream);
        this[to, from].Release(ProxyDirection.UpstreamToClient);
    }

    private static async ValueTask<Stream> ConnectAsync(IPEndPoint endPoint, CancellationToken cancellationToken)
    {
        Socket? socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
            var stream = new NetworkStream(socket, true);
            socket = null;
            return stream;
        }
        finally
        {
            socket?.Dispose();
        }
    }

    /// <summary>Waits for a connect that never completes, as towards a host that drops connection attempts.</summary>
    /// <param name="cancellationToken">The connect token; the connect timeout of the dialing handler cancels it.</param>
    /// <returns>A task that only ever fails with the cancellation.</returns>
    private static async ValueTask<Stream> HangAsync(CancellationToken cancellationToken)
    {
        var never = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        return await never.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static InvalidOperationException UpstreamMismatch(ProxyKey key, TcpPartitionProxy existing, IPEndPoint requested) =>
        new($"The link {key.From} -> {key.To} already has a proxy in front of {existing.Upstream}, not {requested}.");

    private bool IsBlackHoled(string nodeId)
    {
        lock (_gate)
            return _blackHoled.Contains(nodeId);
    }

    private TcpPartitionProxy[] Snapshot()
    {
        lock (_gate)
            return [.. _proxies.Values];
    }

    private async Task<TcpPartitionProxy> StartProxyAsync(ProxyKey key, IPEndPoint upstream, CancellationToken cancellationToken)
    {
        var created = await TcpPartitionProxy.StartAsync(upstream, cancellationToken).ConfigureAwait(false);
        TcpPartitionProxy? surplus = null;
        try
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
                if (_proxies.TryGetValue(key, out var raced))
                {
                    surplus = created;
                    return raced.Upstream.Equals(upstream) ? raced : throw UpstreamMismatch(key, raced, upstream);
                }

                _proxies.Add(key, created);
                return created;
            }
        }
        catch
        {
            surplus = created;
            throw;
        }
        finally
        {
            if (surplus != null)
                await surplus.DisposeAsync().ConfigureAwait(false);
        }
    }

    private bool TryGet(ProxyKey key, IPEndPoint upstream, [MaybeNullWhen(false)] out TcpPartitionProxy proxy)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
            if (!_proxies.TryGetValue(key, out var existing))
            {
                proxy = null;
                return false;
            }

            if (!existing.Upstream.Equals(upstream))
                throw UpstreamMismatch(key, existing, upstream);

            proxy = existing;
            return true;
        }
    }

    /// <summary>A directed node pair.</summary>
    /// <param name="From">The dialing node identifier.</param>
    /// <param name="To">The dialed node identifier.</param>
    [Immutable]
    private readonly record struct ProxyKey(string From, string To)
    {
        internal bool Touches(string nodeId) => string.Equals(From, nodeId, StringComparison.Ordinal) || string.Equals(To, nodeId, StringComparison.Ordinal);
    }
}
