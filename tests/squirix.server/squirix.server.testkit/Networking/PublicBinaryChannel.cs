using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Sends public binary frames while reusing generated protobuf method descriptors in the raw wire tests.</summary>
public sealed class PublicBinaryChannel : IDisposable
{
    private readonly Uri _uri;
    private readonly string? _token;
    private readonly Action<byte>? _acknowledgement;
    private readonly ConcurrentBag<SslStream> _idle = [];
    private readonly Lock _lifetime = new();
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

    internal async Task<T> ExchangeAsync<T>(BinaryOperation operation, IMessage message, MessageParser<T> parser, DateTime? deadline, CancellationToken cancellationToken)
        where T : IMessage<T>
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
            var size = message.CalculateSize();
            var bytes = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes + size);
            var control = (Unsafe.BitCast<BinaryOperation, int>(operation) << 16) | Convert.ToInt32(Math.Floor(Math.Min(budget.TotalMilliseconds, ushort.MaxValue)));
            Frames.WriteMessage(bytes, 0, size, control, message);
            await stream.WriteAsync(bytes, timeout.Token).ConfigureAwait(false);
            var (value, failure) = await ReadReplyAsync(stream, parser, timeout.Token).ConfigureAwait(false);
            lock (_lifetime)
            {
                if (Volatile.Read(ref _disposed) == 0)
                {
                    _idle.Add(stream);
                    stream = null;
                }
            }

            return failure == null ? value! : throw failure;
        }
        catch (OperationCanceledException)
        {
            throw new ServerCallException(cancellationToken.IsCancellationRequested ? FrameStatus.Cancelled : FrameStatus.DeadlineExceeded, "The request was cancelled.");
        }
        catch (Exception error) when (error is InvalidProtocolBufferException or InvalidDataException)
        {
            throw new ServerCallException(FrameStatus.DataLoss, error.Message);
        }
        catch (Exception error) when (error is IOException or SocketException or System.Security.Authentication.AuthenticationException)
        {
            throw new ServerCallException(FrameStatus.Unavailable, error.Message);
        }
        finally
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void ThrowUnexpectedResponseId() => throw new InvalidDataException("Unexpected response ID.");

    private static async Task<(T? Value, ServerCallException? Failure)> ReadReplyAsync<T>(Stream stream, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var (id, size, status) = Frames.Read(header, Frames.MaxResponseBytes);
        if (id != 0)
            ThrowUnexpectedResponseId();

        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, cancellationToken).ConfigureAwait(false);
        return status == 0 ? (parser.ParseFrom(body), null) : (default, new ServerCallException(Unsafe.BitCast<int, FrameStatus>(status), BinaryCallError.Parser.ParseFrom(body)));
    }

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
            using var handler = LoopbackHttp.CreateHandler();
            var tlsOptions = new SslClientAuthenticationOptions
            {
                TargetHost = _uri.IdnHost,
                RemoteCertificateValidationCallback = handler.SslOptions.RemoteCertificateValidationCallback,
            };
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
        const byte anonymous = 0;
        const byte bearer = 1;
        preface[4] = _token == null ? anonymous : bearer;
        await stream.WriteAsync(preface, cancellationToken).ConfigureAwait(false);
        await stream.ReadExactlyAsync(preface.AsMemory(0, 1), cancellationToken).ConfigureAwait(false);
        if (preface[0] is not (Frames.Accepted or Frames.AcceptedWithMutationReplay))
            throw new ServerCallException(FrameStatus.Unauthenticated, "Binary connection credentials are required or unsupported.");
        if (_token == null)
            return;

        var token = System.Text.Encoding.UTF8.GetBytes(_token);
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
