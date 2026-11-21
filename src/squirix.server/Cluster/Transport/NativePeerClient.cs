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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Pools mutually authenticated peer streams, reserving each one for a single request and its reply.</summary>
internal sealed class NativePeerClient : IPeerClient, IDisposable
{
    private readonly DnsEndPoint _address;
    private readonly TrackedConnections _connections;
    private readonly TimeSpan _dial;
    private readonly byte[] _fingerprint;
    private readonly bool _fingerprintIsDigest;
    private readonly List<PeerSession> _idle = [];
    private readonly string _peer;
    private readonly Func<CancellationToken, ValueTask<EndPoint>>? _resolveEndpoint;
    private readonly Lock _sync = new();
    private readonly SslClientAuthenticationOptions _tls;
    private readonly AsyncSemaphore _transfers = new(1);
    private readonly PeerHandshakeVerifier _verifier;
    private bool _closed;
    private ITimer? _idleMonitor;
    private long _lastDialTimeout;

    internal NativePeerClient((Uri Address, string Id) target, MtlsCertificate material, PeerHandshakeVerifier verifier, TrackedConnections connections, TimeSpan dial, string? fingerprint, PeerSocketOptions? options = null)
    {
        var (address, peer) = target;
        _address = new DnsEndPoint(address.DnsSafeHost, address.Port);
        _peer = peer;
        _verifier = verifier;
        _connections = connections;
        _dial = dial;
        (_fingerprint, _fingerprintIsDigest) = PeerFrames.EncodeFingerprint(fingerprint);
        _resolveEndpoint = options?.ResolveEndpoint;
        _tls = options?.Tls ?? CreateTlsOptions(address.DnsSafeHost, peer, material, verifier);
        if (options == null)
            return;
        _tls.TargetHost = address.DnsSafeHost;
        _tls.ApplicationProtocols = [PeerFrames.Protocol];
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

    public Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage
    {
        return operation == PeerOperation.TransferReplicaState
            ? TransferAsync(operation, request, parser, cancellationToken)
            : ExchangeAsync(operation, request, parser, cancellationToken);
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

    internal static async Task<T> ReadReplyAsync<T>(Stream stream, WireParser<T> parser, Memory<byte> header, CancellationToken cancellationToken)
        where T : WireMessage
    {
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        (int Status, int Size) dimensions;
        try
        {
            dimensions = PeerFrames.ReadReply(header.Span);
        }
        catch (InvalidDataException error)
        {
            throw new ServerCallException(FrameStatus.DataLoss, "Malformed peer response header.", error);
        }

        var (status, size) = dimensions;
        try
        {
            byte? prefix = null;
            if (status == 0 && size > Frames.MaxBatchBytes)
            {
                if (ReferenceEquals(parser, GetDistributedAsyncResponse.Parser))
                    return await ReadOwnedReplyAsync(stream, parser, size, null, cancellationToken).ConfigureAwait(false);

                if (ReferenceEquals(parser, GetValueAsyncResponse.Parser))
                {
                    await stream.ReadExactlyAsync(header[..1], cancellationToken).ConfigureAwait(false);
                    prefix = header.Span[0];
                    if (GetValueAsyncResponse.HasRawPayload(prefix.Value))
                        return await ReadOwnedReplyAsync(stream, parser, size, prefix, cancellationToken).ConfigureAwait(false);
                }
            }

            var buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                var offset = PlaceReplyPrefix(buffer, prefix);
                await stream.ReadExactlyAsync(buffer.AsMemory(offset, size - offset), cancellationToken).ConfigureAwait(false);
                return status == 0 ? parser.ParseFrom(buffer, 0, size) : throw new ServerCallException(Unsafe.BitCast<int, FrameStatus>(status), BinaryCallError.Parser.ParseFrom(buffer, 0, size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, true);
            }
        }
        catch (InvalidDataException)
        {
            throw new ServerCallException(FrameStatus.DataLoss, "Malformed peer response.");
        }
    }

    internal static string TraceText(string? value, int limit, out int byteCount)
    {
        if (!string.IsNullOrEmpty(value) && value.Length <= limit)
        {
            try
            {
                var count = PeerFrames.Text.GetByteCount(value);
                if (count <= limit)
                {
                    byteCount = count;
                    return value;
                }
            }
            catch (EncoderFallbackException)
            {
                // A malformed activity value is omitted from the peer frame.
            }
        }

        byteCount = 0;
        return string.Empty;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<T> ReadOwnedReplyAsync<T>(Stream stream, WireParser<T> parser, int size, byte? prefix, CancellationToken cancellationToken)
        where T : WireMessage
    {
        var body = GC.AllocateUninitializedArray<byte>(size);
        var offset = PlaceReplyPrefix(body, prefix);
        await stream.ReadExactlyAsync(body.AsMemory(offset), cancellationToken).ConfigureAwait(false);

        // The response may outlive this connection while its bytes are encoded for the SDK.
        return parser.ParseBorrowed(body);
    }

    private static int PlaceReplyPrefix(byte[] buffer, byte? prefix)
    {
        if (prefix is not { } value)
            return 0;

        buffer[0] = value;
        return 1;
    }

    private static long? RemainingBudget()
    {
        var ticks = ServerRpcDeadlineContext.CurrentBudget?.Remaining.Ticks;
        return ticks <= 0 ? throw new ServerCallException(FrameStatus.DeadlineExceeded, "Request deadline exceeded.") : ticks;
    }

    private ITimer CreateIdleMonitor()
    {
        var interval = TimeSpan.FromSeconds(1);

        if (ExecutionContext.IsFlowSuppressed())
            return Start();
        using var suppression = ExecutionContext.SuppressFlow();
        return Start();

        ITimer Start()
        {
            return TimeProvider.System.CreateTimer(static state => (state as NativePeerClient)?.RemoveClosedSessions(), this, interval, interval);
        }
    }

    private async Task<T> ExchangeAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage
    {
        _ = RemainingBudget();
        var size = request.CalculateSize();
        if (size > EntryLimits.MaxSendMessageSizeBytes)
            throw new ServerCallException(FrameStatus.ResourceExhausted, "The request exceeds the peer message limit.");

        var activity = Activity.Current;
        var context = activity is { IdFormat: ActivityIdFormat.W3C } ? activity.Context : default;
        var binaryParent = context.TraceId != default && context.SpanId != default;
        var parentBytes = WireTraceParent.ByteCount;
        var parent = binaryParent ? string.Empty : TraceText(activity?.Id, Frames.MaxTraceParentLength, out parentBytes);
        var state = TraceText(activity?.TraceStateString, Frames.MaxTraceStateLength, out var stateBytes);
        var sent = false;
        PeerSession? stream = null;
        try
        {
            stream = await RentAsync(cancellationToken).ConfigureAwait(false);
            var budget = RemainingBudget();
            var frame = new PeerFrames.Request(size, budget ?? -1, operation, _fingerprint.Length, parentBytes, stateBytes, _fingerprintIsDigest, binaryParent);
            var length = checked(PeerFrames.RequestBytes + frame.Metadata + size);
            var packet = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                PeerFrames.WriteRequest(packet, in frame);
                _fingerprint.AsSpan().CopyTo(packet.AsSpan(PeerFrames.RequestBytes));
                var offset = PeerFrames.RequestBytes + _fingerprint.Length;
                if (binaryParent)
                    WireTraceParent.Write(packet.AsSpan(offset), in context);
                else
                    _ = PeerFrames.Text.GetBytes(parent, packet.AsSpan(offset));

                offset += parentBytes;
                offset += PeerFrames.Text.GetBytes(state, packet.AsSpan(offset));
                request.WriteTo(packet.AsSpan(offset, size));
                cancellationToken.ThrowIfCancellationRequested();
                stream.RequestStarted = Stopwatch.GetTimestamp();
                sent = true;
                await stream.WriteAsync(packet.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(packet, true);
            }

            var reply = await ReadReplyAsync(stream, parser, stream.ReplyHeader, cancellationToken).ConfigureAwait(false);
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

            if (_idle.Count != 0)
                return;
            _idleMonitor?.Dispose();
            _idleMonitor = null;
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
                    if (stream is { CanReuse: true, RemoteCertificate: X509Certificate2 cert } && _verifier.VerifyExpectedPeer(cert, _peer))
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

    private async Task<T> TransferAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage
    {
        _ = RemainingBudget();
        await _transfers.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ExchangeAsync(operation, request, parser, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _transfers.Release();
        }
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
                    return Stopwatch.GetElapsedTime(RequestStarted) < PeerFrames.IdleTimeout / 2 && InnerStream is NetworkStream network && !network.Socket.Poll(0, SelectMode.SelectRead);
                }
                catch (Exception error) when (error is SocketException or ObjectDisposedException)
                {
                    return false;
                }
            }
        }

        internal Memory<byte> ReplyHeader { get; } = new byte[PeerFrames.ReplyBytes];

        /// <summary>Gets or sets the timestamp before the last request was sent, a lower bound for when the peer started its idle timer.</summary>
        internal long RequestStarted { get; set; }
    }
}
