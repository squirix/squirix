using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Sends public binary frames while reusing cache message parsers in the raw wire tests.</summary>
public sealed class PublicBinaryChannel : IDisposable
{
    private readonly Action<byte>? _acknowledgement;
    private readonly ConcurrentBag<SslStream> _idle = [];
    private readonly Lock _lifetime = new();
    private readonly string? _token;
    private readonly Uri _uri;
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="PublicBinaryChannel" /> class.</summary>
    /// <param name="uri">The HTTPS endpoint.</param>
    /// <param name="token">Optional connection credential.</param>
    /// <param name="acknowledgement">Optional observer of the JWT handshake response.</param>
    public PublicBinaryChannel(Uri uri, string? token = null, Action<byte>? acknowledgement = null)
    {
        _uri = uri;
        _token = token;
        _acknowledgement = acknowledgement;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_lifetime)
        {
            _ = Interlocked.Exchange(ref _disposed, 1);
            while (_idle.TryTake(out var stream))
                stream.Dispose();
        }
    }

    internal async Task<T> ExchangeAsync<T>(BinaryOperation operation, WireMessage message, WireParser<T> parser, DateTime? deadline, CancellationToken cancellationToken)
        where T : WireMessage
    {
        var budget = deadline is { } end ? end - TimeProvider.System.GetUtcNow().UtcDateTime : TimeSpan.FromSeconds(30);
        if (budget <= TimeSpan.Zero)
            throw new ServerCallException(FrameStatus.DeadlineExceeded, "The request deadline elapsed.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(budget);
        SslStream? stream = null;
        try
        {
            stream = _idle.TryTake(out var pooled) ? pooled : await ConnectAsync(timeout.Token).ConfigureAwait(false);
            var control = (Unsafe.BitCast<BinaryOperation, int>(operation) << 16) | Convert.ToInt32(Math.Floor(Math.Min(budget.TotalMilliseconds, ushort.MaxValue)));
            await WriteRequestAsync(stream, message, control, timeout.Token).ConfigureAwait(false);
            var (value, failure) = await ReadReplyAsync(stream, parser, timeout.Token).ConfigureAwait(false);
            lock (_lifetime)
            {
                if (Volatile.Read(ref _disposed) != 0)
                    return failure == null ? value! : throw failure;
                _idle.Add(stream);
                stream = null;
            }

            return failure == null ? value! : throw failure;
        }
        catch (OperationCanceledException)
        {
            throw new ServerCallException(cancellationToken.IsCancellationRequested ? FrameStatus.Cancelled : FrameStatus.DeadlineExceeded, "The request was cancelled.");
        }
        catch (InvalidDataException error)
        {
            throw new ServerCallException(FrameStatus.DataLoss, error.Message);
        }
        catch (Exception error) when (error is IOException or SocketException or AuthenticationException)
        {
            throw new ServerCallException(FrameStatus.Unavailable, error.Message);
        }
        finally
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal async Task<ServerCallException?> ReadWriteHeaderRejectionAsync(BinaryOperation operation, int valueBytes, CancellationToken cancellationToken)
    {
        await using var stream = await ConnectAsync(cancellationToken).ConfigureAwait(false);
        var frame = GC.AllocateUninitializedArray<byte>(Frames.RequestBytes + Frames.ExtendedRequestBytes);
        var control = (Unsafe.BitCast<BinaryOperation, int>(operation) << 16) | 15000;
        Frames.WriteRequest(frame, 0, Frames.ExtendedSize, control);
        Frames.WriteExtendedSize(frame.AsSpan(Frames.RequestBytes), valueBytes + 64);
        Frames.WriteExtendedSize(frame.AsSpan(Frames.RequestBytes + sizeof(int)), valueBytes);
        await stream.WriteAsync(frame, cancellationToken).ConfigureAwait(false);
        var (_, failure) = await ReadReplyAsync(stream, SetAsyncResponse.Parser, cancellationToken).ConfigureAwait(false);
        return failure;
    }

    private static async ValueTask WriteRequestAsync(Stream stream, WireMessage message, int control, CancellationToken cancellationToken)
    {
        var size = message.CalculateSize();
        if (size <= Frames.MaxRequestBytes)
        {
            var bytes = GC.AllocateUninitializedArray<byte>(Frames.RequestBytes + size);
            Frames.WriteRequestMessage(bytes, 0, size, control, message);
            await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            return;
        }

        using var segments = new WireSegments();
        const int headerSize = Frames.RequestBytes + Frames.ExtendedRequestBytes;
        var header = segments.GetMemory(headerSize);
        Frames.WriteRequest(header.Span, 0, Frames.ExtendedSize, control);
        Frames.WriteExtendedSize(header.Span[Frames.RequestBytes..], size);
        Frames.WriteExtendedSize(header.Span[(Frames.RequestBytes + sizeof(int))..], CacheWritePayload.Length(message) ?? 0);
        segments.Advance(headerSize);
        message.WriteTo(segments);
        await segments.SendAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(T? Value, ServerCallException? Failure)> ReadReplyAsync<T>(Stream stream, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.ReplyBytes);
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var (id, size, status) = Frames.ReadReply(header);
        if (id != 0)
            ThrowUnexpectedResponseId();

        if (size == Frames.ExtendedSize)
        {
            await stream.ReadExactlyAsync(header.AsMemory(0, Frames.ExtendedReplyBytes), cancellationToken).ConfigureAwait(false);
            size = Frames.ReadExtendedSize(header);
        }

        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, cancellationToken).ConfigureAwait(false);
        return status == 0 ? (parser.ParseBorrowed(body), null) : (default, new ServerCallException(Unsafe.BitCast<int, FrameStatus>(status), BinaryCallError.Parser.ParseFrom(body)));
    }

    private static void ThrowUnexpectedResponseId() => throw new InvalidDataException("Unexpected response ID.");

#pragma warning disable CA2000 // The returned TLS stream owns the socket through NetworkStream.
    private async Task<SslStream> ConnectAsync(CancellationToken cancellationToken)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        NetworkStream? network = null;
        SslStream? stream = null;
        try
        {
            await socket.ConnectAsync(_uri.IdnHost, _uri.Port, cancellationToken).ConfigureAwait(false);
            network = new NetworkStream(socket, true);
            stream = new SslStream(network);
            var tlsOptions = LoopbackHttp.CreateTlsOptions();
            tlsOptions.TargetHost = _uri.IdnHost;
            await stream.AuthenticateAsClientAsync(tlsOptions, cancellationToken).ConfigureAwait(false);
            await NegotiateAsync(stream, cancellationToken).ConfigureAwait(false);
            return stream;
        }
        catch
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
            else if (network != null)
                await network.DisposeAsync().ConfigureAwait(false);
            else
                socket.Dispose();
            throw;
        }
    }

#pragma warning restore CA2000

    private async Task NegotiateAsync(Stream stream, CancellationToken cancellationToken)
    {
        var preface = GC.AllocateArray<byte>(Frames.PrefaceBytes);
        BinaryPrimitives.WriteUInt32BigEndian(preface, Frames.Preface);
        const byte bearer = Frames.ExtendedMode | 1;
        preface[4] = _token == null ? Frames.ExtendedMode : bearer;
        await stream.WriteAsync(preface, cancellationToken).ConfigureAwait(false);
        await stream.ReadExactlyAsync(preface.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
        if (preface[0] is not (Frames.Accepted or Frames.AcceptedWithMutationReplay))
            throw new ServerCallException(FrameStatus.Unauthenticated, "Binary connection credentials are required or unsupported.");
        if (_token == null)
            return;

        var token = Encoding.UTF8.GetBytes(_token);
        var header = GC.AllocateUninitializedArray<byte>(ConnectionAuthenticationProtocol.HeaderLength);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, token.Length);
        await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(token, cancellationToken).ConfigureAwait(false);
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        if (!ConnectionAuthenticationProtocol.TryReadAcknowledgement(header, out var status))
            throw new IOException("Invalid JWT acknowledgement.");
        _acknowledgement?.Invoke(status);
        if (status != ConnectionAuthenticationProtocol.Accepted)
            throw new IOException("JWT authentication rejected.");
    }
}
