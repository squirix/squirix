using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Pools mutually authenticated peer streams, reserving each one for a single request and its reply.</summary>
internal sealed class NativePeerClient : IPeerClient, IDisposable
{
    private readonly AsyncSemaphore _transfers = new(1);
    private readonly Lock _sync = new();
    private readonly List<PeerSession> _idle = [];
    private readonly DnsEndPoint _address;
    private readonly Func<CancellationToken, ValueTask<EndPoint>>? _resolveEndpoint;
    private readonly SslClientAuthenticationOptions _tls;
    private readonly PeerHandshakeVerifier _verifier;
    private readonly string _peer;
    private readonly string _fingerprint;
    private readonly TrackedConnections _connections;
    private readonly TimeSpan _dial;
    private ITimer? _idleMonitor;
    private bool _closed;
    private long _lastDialTimeout;

    internal NativePeerClient((Uri Address, string Id) target, MtlsCertificate material, PeerHandshakeVerifier verifier, TrackedConnections connections, TimeSpan dial, string? fingerprint, PeerSocketOptions? options = null)
    {
        var (address, peer) = target;
        _address = new DnsEndPoint(address.DnsSafeHost, address.Port);
        _peer = peer;
        _verifier = verifier;
        _connections = connections;
        _dial = dial;
        _fingerprint = fingerprint ?? string.Empty;
        _resolveEndpoint = options?.ResolveEndpoint;
        _tls = options?.Tls ?? CreateTlsOptions(address.DnsSafeHost, peer, material, verifier);
        if (options != null)
        {
            _tls.TargetHost = address.DnsSafeHost;
            _tls.ApplicationProtocols = [PeerFrames.Protocol];
        }
    }

    public Task<T> SendAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        return string.Equals(path, "replication/TransferReplicaState", StringComparison.Ordinal)
            ? TransferAsync(path, request, parser, cancellationToken)
            : ExchangeAsync(path, request, parser, cancellationToken);
    }

    public void Dispose()
    {
        _transfers.Dispose();
        lock (_sync)
        {
            _closed = true;
            _idleMonitor?.Dispose();
            for (var i = 0; i < _idle.Count; i++)
                _idle[i].Dispose();
            _idle.Clear();
        }
    }

    internal static SslClientAuthenticationOptions CreateTlsOptions(string host, string peer, MtlsCertificate material, PeerHandshakeVerifier verifier)
    {
        var cert = material.NodeCertificate!;
        return new SslClientAuthenticationOptions
        {
            TargetHost = host,
            ClientCertificates = [cert],
            ClientCertificateContext = OperatingSystem.IsWindows() ? null : SslStreamCertificateContext.Create(cert, null, true),
            ApplicationProtocols = [PeerFrames.Protocol],
            RemoteCertificateValidationCallback = (_, presented, _, _) => presented is X509Certificate2 certificate && verifier.VerifyExpectedPeer(certificate, peer),
        };
    }

    private static async Task<T> ReadReplyAsync<T>(SslStream stream, MessageParser<T> parser, Memory<byte> header, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var (status, size) = PeerFrames.ReadReply(header.Span);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            await stream.ReadExactlyAsync(buffer.AsMemory(0, size), cancellationToken).ConfigureAwait(false);
            return status == 0 ? parser.ParseFrom(buffer, 0, size) : throw new ServerCallException(Unsafe.BitCast<int, FrameStatus>(status), BinaryCallError.Parser.ParseFrom(buffer, 0, size));
        }
        catch (InvalidProtocolBufferException)
        {
            throw new ServerCallException(FrameStatus.DataLoss, "Malformed peer response.");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, true);
        }
    }

    private async Task<T> TransferAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        await _transfers.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ExchangeAsync(path, request, parser, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _transfers.Release();
        }
    }

    private async Task<T> ExchangeAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        var size = request.CalculateSize();
        if (size > EntryLimits.MaxSendMessageSizeBytes)
            throw new ServerCallException(FrameStatus.ResourceExhausted, "The request exceeds the peer message limit.");

        var parent = Activity.Current?.Id ?? string.Empty;
        var state = Activity.Current?.TraceStateString ?? string.Empty;
        var sent = false;
        PeerSession? stream = null;
        try
        {
            stream = await RentAsync(cancellationToken).ConfigureAwait(false);
            var budget = ServerRpcDeadlineContext.CurrentBudget?.Remaining.Ticks;
            if (budget <= 0)
                throw new ServerCallException(FrameStatus.DeadlineExceeded, "Request deadline exceeded.");

            var frame = new PeerFrames.Request(size, budget ?? -1, PeerFrames.Text.GetByteCount(path), PeerFrames.Text.GetByteCount(_fingerprint), PeerFrames.Text.GetByteCount(parent), PeerFrames.Text.GetByteCount(state));
            var length = checked(PeerFrames.RequestBytes + frame.Metadata + size);
            T reply;
            var packet = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                PeerFrames.WriteRequest(packet, in frame);
                var offset = PeerFrames.RequestBytes;
                offset += PeerFrames.Text.GetBytes(path, packet.AsSpan(offset));
                offset += PeerFrames.Text.GetBytes(_fingerprint, packet.AsSpan(offset));
                offset += PeerFrames.Text.GetBytes(parent, packet.AsSpan(offset));
                offset += PeerFrames.Text.GetBytes(state, packet.AsSpan(offset));
                request.WriteTo(packet.AsSpan(offset, size));
                cancellationToken.ThrowIfCancellationRequested();
                sent = true;
                await stream.WriteAsync(packet.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                reply = await ReadReplyAsync(stream, parser, packet.AsMemory(0, PeerFrames.ReplyBytes), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(packet, true);
            }

            await ReturnAsync(stream).ConfigureAwait(false);
            stream = null;
            return reply;
        }
        catch (Exception error) when (error is IOException or SocketException or AuthenticationException)
        {
            throw new ServerCallException(FrameStatus.Unavailable, "Peer request failed.", sent ? null : new HttpRequestException(HttpRequestError.ConnectionError, "Could not establish the peer connection.", error));
        }
        finally
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask<PeerSession> RentAsync(CancellationToken cancellationToken)
    {
        var idle = TakeIdle();
        if (idle != null)
            return idle;

        var timedOut = Interlocked.Read(ref _lastDialTimeout);
        if (timedOut != 0 && Stopwatch.GetElapsedTime(timedOut) < _dial)
            throw new SocketException(Unsafe.BitCast<SocketError, int>(SocketError.TimedOut));

        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        handshake.CancelAfter(TimeSpan.FromSeconds(5));
        using var dial = CancellationTokenSource.CreateLinkedTokenSource(handshake.Token);
        dial.CancelAfter(_dial);
        Stream network;
        try
        {
            var endpoint = _resolveEndpoint == null ? _address : await _resolveEndpoint(dial.Token).ConfigureAwait(false);
            network = await TrackedConnectionStream.ConnectAsync(_connections, endpoint, dial.Token).ConfigureAwait(false);
            _ = Interlocked.Exchange(ref _lastDialTimeout, 0);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            if (_fingerprint.Length != 0)
                _ = Interlocked.Exchange(ref _lastDialTimeout, Stopwatch.GetTimestamp());
            throw new SocketException(Unsafe.BitCast<SocketError, int>(SocketError.TimedOut));
        }

        PeerSession? connection = new(network);
        try
        {
            await connection.AuthenticateAsClientAsync(_tls, handshake.Token).ConfigureAwait(false);
            if (connection.NegotiatedApplicationProtocol != PeerFrames.Protocol)
                throw new AuthenticationException("The node did not negotiate the required Squirix peer protocol.");
            var owned = connection;
            connection = null;
            return owned;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new SocketException(Unsafe.BitCast<SocketError, int>(SocketError.TimedOut));
        }
        finally
        {
            if (connection != null)
                await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private PeerSession? TakeIdle()
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            while (_idle.Count > 0)
            {
                var stream = _idle[^1];
                _idle.RemoveAt(_idle.Count - 1);
                try
                {
                    if (stream.CanReuse && stream.RemoteCertificate is X509Certificate2 cert && _verifier.VerifyExpectedPeer(cert, _peer))
                    {
                        var owned = stream;
                        stream = null;
                        return owned;
                    }
                }
                finally
                {
                    stream?.Dispose();
                }
            }
        }

        return null;
    }

    private async ValueTask ReturnAsync(PeerSession stream)
    {
        lock (_sync)
        {
            if (!_closed && _idle.Count < 16)
            {
                _idleMonitor ??= CreateIdleMonitor();
                _idle.Add(stream);
                return;
            }
        }

        await stream.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Closes a half-closed idle stream even if the node sends no further calls to its peer.</summary>
    private void RemoveClosedSessions()
    {
        lock (_sync)
        {
            for (var i = _idle.Count - 1; i >= 0; i--)
            {
                var stream = _idle[i];
                if (stream.CanReuse)
                    continue;

                _idle.RemoveAt(i);
                stream.Dispose();
            }

            if (_idle.Count == 0)
            {
                _idleMonitor?.Dispose();
                _idleMonitor = null;
            }
        }
    }

    private ITimer CreateIdleMonitor()
    {
        var interval = TimeSpan.FromSeconds(1);
        ITimer Start()
        {
            return TimeProvider.System.CreateTimer(static state => (state as NativePeerClient)?.RemoveClosedSessions(), this, interval, interval);
        }

        if (ExecutionContext.IsFlowSuppressed())
            return Start();
        using var suppression = ExecutionContext.SuppressFlow();
        return Start();
    }

    private sealed class PeerSession : SslStream
    {
        internal PeerSession(Stream network)
            : base(network)
        {
        }

        /// <summary>Gets a value indicating whether the idle socket has neither a pending shutdown nor an unexpected frame.</summary>
        internal bool CanReuse
        {
            get
            {
                try
                {
                    return InnerStream is NetworkStream network && !network.Socket.Poll(0, SelectMode.SelectRead);
                }
                catch (Exception error) when (error is SocketException or ObjectDisposedException)
                {
                    return false;
                }
            }
        }
    }
}
