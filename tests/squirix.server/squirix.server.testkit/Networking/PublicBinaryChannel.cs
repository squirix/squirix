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
using Grpc.Core;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Sends public binary frames while reusing generated protobuf method descriptors in the raw wire tests.</summary>
public sealed class PublicBinaryChannel : CallInvoker, IDisposable
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

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        ArgumentNullException.ThrowIfNull(method);
        if (options.Headers is { Count: > 0 })
            ThrowUnsupportedMetadata();

        var lifetime = new CallLifetime(options.CancellationToken);
        return new AsyncUnaryCall<TResponse>(
            UnwrapAsync(ExchangeAsync(method, options.WithCancellationToken(lifetime.Token), request), lifetime),
            Task.FromResult(new Metadata()),
            static () => throw new NotSupportedException(),
            static () => throw new NotSupportedException(),
            lifetime.Cancel);
    }

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    private static void ThrowUnsupportedMetadata() => throw new NotSupportedException("The public binary test channel has no request metadata.");

    private static async Task<T> UnwrapAsync<T>(Task<Reply<T>> response, CallLifetime lifetime)
    {
        await using var owned = lifetime.ConfigureAwait(false);
        var reply = await response.ConfigureAwait(false);
        return reply.Status.StatusCode == StatusCode.OK ? reply.Value! : throw new RpcException(reply.Status, reply.Trailers);
    }

    private static Reply<T> Failure<T>(StatusCode status, string detail) => new(default, new Status(status, detail), []);

    private static Reply<T> ParseReply<T>(BinaryOperation operation, int control, byte[] body)
    {
        if (control == 0)
        {
            var value = ParseMessage(operation, body);
            return value is T typed ? new Reply<T>(typed, Status.DefaultSuccess, []) : Failure<T>(StatusCode.DataLoss, "Unexpected protobuf response type.");
        }

        var status = Unsafe.BitCast<int, StatusCode>(control);
        var error = BinaryCallError.Parser.ParseFrom(body);
        var trailers = new Metadata();
        foreach (var entry in error.Trailers)
        {
            if (entry.ValueCase == BinaryCallError.Types.Trailer.ValueOneofCase.Binary)
                trailers.Add(entry.Key, entry.Binary.ToByteArray());
            else
                trailers.Add(entry.Key, entry.Text);
        }

        return new Reply<T>(default, new Status(status, error.Detail), trailers);
    }

    private static IMessage? ParseMessage(BinaryOperation operation, byte[] body) => operation switch
    {
        BinaryOperation.SetEntry => SetAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.GetEntry => GetEntryAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.GetValue => GetValueAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.Remove => RemoveAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.TryAddEntry => TryAddAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.Touch => TouchAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.RemoveExpiration => RemoveExpirationAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.Update => UpdateAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.GetExpiration => GetExpirationAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.GetOrAdd => GetOrAddAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.SetDistributed => SetAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.GetDistributed => GetDistributedAsyncResponse.Parser.ParseFrom(body),
        BinaryOperation.RefreshDistributed => RefreshDistributedAsyncResponse.Parser.ParseFrom(body),
        _ => null,
    };

    private static void ThrowUnexpectedResponseId() => throw new InvalidDataException("Unexpected response ID.");

    private static async Task<Reply<T>> ReadReplyAsync<T>(Stream stream, BinaryOperation operation, CancellationToken cancellationToken)
    {
        var header = GC.AllocateUninitializedArray<byte>(Frames.HeaderBytes);
        await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        var (id, size, status) = Frames.Read(header, Frames.MaxResponseBytes);
        if (id != 0)
            ThrowUnexpectedResponseId();

        var body = GC.AllocateUninitializedArray<byte>(size);
        await stream.ReadExactlyAsync(body, cancellationToken).ConfigureAwait(false);
        return ParseReply<T>(operation, status, body);
    }

    private async Task<Reply<TResponse>> ExchangeAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method, CallOptions options, TRequest request)
    {
        var now = TimeProvider.System.GetUtcNow().UtcDateTime;
        var budget = options.Deadline is { } deadline ? deadline - now : TimeSpan.FromSeconds(30);
        if (budget <= TimeSpan.Zero)
            return Failure<TResponse>(StatusCode.DeadlineExceeded, "The request deadline elapsed.");

        if (!Enum.TryParse<BinaryOperation>(method.Name, out var operation) || request is not IMessage message)
            return Failure<TResponse>(StatusCode.Unimplemented, method.Name);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);
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
            var result = await ReadReplyAsync<TResponse>(stream, operation, timeout.Token).ConfigureAwait(false);
            lock (_lifetime)
            {
                if (Volatile.Read(ref _disposed) != 0)
                    return result;

                _idle.Add(stream);
                stream = null;
            }

            return result;
        }
        catch (RpcException error)
        {
            return new Reply<TResponse>(default, error.Status, error.Trailers);
        }
        catch (OperationCanceledException)
        {
            return Failure<TResponse>(options.CancellationToken.IsCancellationRequested ? StatusCode.Cancelled : StatusCode.DeadlineExceeded, "The request was cancelled.");
        }
        catch (Exception error) when (error is InvalidProtocolBufferException or InvalidDataException)
        {
            return Failure<TResponse>(StatusCode.DataLoss, error.Message);
        }
        catch (Exception error) when (error is IOException or SocketException or System.Security.Authentication.AuthenticationException)
        {
            return Failure<TResponse>(StatusCode.Unavailable, error.Message);
        }
        finally
        {
            if (stream != null)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
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
        if (preface[0] != Frames.Accepted)
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Binary connection credentials are required or unsupported."));
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

    private readonly record struct Reply<T>(T? Value, Status Status, Metadata Trailers);

    private sealed class CallLifetime : IAsyncDisposable
    {
        private readonly Lock _gate = new();
        private readonly CancellationTokenSource _source;
        private Task? _cancellation;
        private bool _finished;

        internal CallLifetime(CancellationToken callerToken)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
        }

        internal CancellationToken Token => _source.Token;

        public async ValueTask DisposeAsync()
        {
            Task? cancellation;
            lock (_gate)
            {
                _finished = true;
                cancellation = _cancellation;
            }

            try
            {
                if (cancellation != null)
                    await cancellation.ConfigureAwait(false);
            }
            finally
            {
                _source.Dispose();
            }
        }

        internal void Cancel()
        {
            lock (_gate)
            {
                if (!_finished)
                    _cancellation ??= _source.CancelAsync();
            }
        }
    }
}
