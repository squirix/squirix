using System;
using System.Buffers;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class BinaryCacheClient : IAsyncDisposable
{
    private readonly Uri _endpoint;
    private readonly Func<CancellationToken, ValueTask<string>>? _tokenProvider;
    private readonly SslClientAuthenticationOptions? _tlsOptions;
    private readonly Func<BinaryOperation, IMessage, MessageParser, BinaryCallOptions, Task<IMessage>>? _invoke;
    private readonly Channel<bool> _connect = Channel.CreateBounded<bool>(1);
    private Connection? _connection;
    private int _disposed;

    internal BinaryCacheClient(
        Uri endpoint,
        Func<CancellationToken, ValueTask<string>>? tokenProvider,
        SslClientAuthenticationOptions? tlsOptions,
        Func<BinaryOperation, IMessage, MessageParser, BinaryCallOptions, Task<IMessage>>? invoke = null)
    {
        _endpoint = endpoint;
        _tokenProvider = tokenProvider;
        _tlsOptions = tlsOptions;
        _invoke = invoke;
        _ = _connect.Writer.TryWrite(true);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _ = await _connect.Reader.ReadAsync().ConfigureAwait(false);
        try
        {
            if (_connection != null)
                await _connection.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _ = _connect.Writer.TryWrite(true);
        }
    }

    internal static void RequireHttps(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        if (!uri.IsAbsoluteUri || !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Squirix transport requires absolute HTTPS endpoints.", nameof(uri));
    }

    internal async Task ConnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            _ = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception error) when (error is AuthenticationException or ConnectionRejectedException)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unauthenticated, error.Message, error));
        }
        catch (Exception error) when (error is IOException or SocketException)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "Connection failed.", error));
        }
    }

    internal Task<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.SetEntry, request, SetAsyncResponse.Parser, options);

    internal Task<GetEntryAsyncResponse> GetEntryAsync(GetEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetEntry, request, GetEntryAsyncResponse.Parser, options);

    internal Task<GetValueAsyncResponse> GetValueAsync(GetValueAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetValue, request, GetValueAsyncResponse.Parser, options);

    internal Task<RemoveAsyncResponse> RemoveAsync(RemoveAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Remove, request, RemoveAsyncResponse.Parser, options);

    internal Task<TryAddAsyncResponse> AddEntryIfMissingAsync(TryAddEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.TryAddEntry, request, TryAddAsyncResponse.Parser, options);

    internal Task<TouchAsyncResponse> TouchAsync(TouchAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Touch, request, TouchAsyncResponse.Parser, options);

    internal Task<RemoveExpirationAsyncResponse> RemoveExpirationAsync(RemoveExpirationAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.RemoveExpiration, request, RemoveExpirationAsyncResponse.Parser, options);

    internal Task<UpdateAsyncResponse> UpdateAsync(UpdateAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Update, request, UpdateAsyncResponse.Parser, options);

    internal Task<GetExpirationAsyncResponse> GetExpirationAsync(GetExpirationAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetExpiration, request, GetExpirationAsyncResponse.Parser, options);

    internal Task<GetOrAddAsyncResponse> GetOrAddAsync(GetOrAddAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetOrAdd, request, GetOrAddAsyncResponse.Parser, options);

    internal Task<SetAsyncResponse> SetDistributedAsync(SetDistributedAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.SetDistributed, request, SetAsyncResponse.Parser, options);

    internal Task<GetDistributedAsyncResponse> GetDistributedAsync(GetDistributedAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetDistributed, request, GetDistributedAsyncResponse.Parser, options);

    internal Task<RefreshDistributedAsyncResponse> RefreshDistributedAsync(RefreshDistributedAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.RefreshDistributed, request, RefreshDistributedAsyncResponse.Parser, options);

    private static T ThrowUnexpectedResponse<T>() => throw new InvalidDataException("Unexpected cache response type.");

    private async Task<T> CallAsync<T>(BinaryOperation operation, IMessage request, MessageParser<T> parser, BinaryCallOptions options)
        where T : class, IMessage<T>
    {
        if (_invoke != null)
        {
            var response = await _invoke(operation, request, parser, options).ConfigureAwait(false);
            return response as T ?? ThrowUnexpectedResponse<T>();
        }

        var (control, wireRequest, bodySize) = BinaryClientProtocol.PrepareRequest(operation, request);

        Connection? connection = null;
        try
        {
            connection = await GetConnectionAsync(options.CancellationToken).ConfigureAwait(false);
            var preventReplay = !connection.SupportsMutationReplay && operation is not (BinaryOperation.GetEntry or BinaryOperation.GetValue or BinaryOperation.GetExpiration or BinaryOperation.GetDistributed);
            var result = await connection.CallAsync(control, wireRequest, bodySize, parser, options, preventReplay).ConfigureAwait(false);
            return result as T ?? ThrowUnexpectedResponse<T>();
        }
        catch (Exception error) when (error is AuthenticationException or ConnectionRejectedException)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unauthenticated, error.Message, error));
        }
        catch (OperationCanceledException error) when (connection == null && !options.CallerToken.IsCancellationRequested)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "Unable to establish the binary connection within the attempt budget.", error));
        }
        catch (Exception error) when (error is IOException or SocketException or InvalidDataException)
        {
            connection?.Abort();
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "Binary connection closed.", error));
        }
    }

    private async ValueTask<Connection> GetConnectionAsync(CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _connection) is { IsAvailable: true } existing)
            return existing;

        _ = await _connect.Reader.ReadAsync(token).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_connection is { IsAvailable: true } current)
                return current;
            if (_connection != null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                Volatile.Write(ref _connection, null);
            }

            var connected = await Connection.ConnectAsync(_endpoint, _tokenProvider, _tlsOptions, token).ConfigureAwait(false);
            Volatile.Write(ref _connection, connected);
            return connected;
        }
        finally
        {
            _ = _connect.Writer.TryWrite(true);
        }
    }

    private sealed class Connection : IAsyncDisposable
    {
        private const int PendingCapacity = 128;

        private readonly TcpClient _socket;
        private readonly SslStream _tls;
        private readonly CancellationTokenSource _stop = new();
        private readonly CancellationToken _stopToken;
        private readonly Channel<int> _slots = Channel.CreateBounded<int>(PendingCapacity);
        private readonly Channel<PendingCall> _outgoing = Channel.CreateBounded<PendingCall>(new BoundedChannelOptions(PendingCapacity) { SingleReader = true });
        private readonly PendingCall?[] _pending = new PendingCall?[PendingCapacity];
        private readonly Task _reader;
        private readonly Task _sender;
        private int _aborted;

        private Connection(TcpClient socket, SslStream tls, bool supportsMutationReplay)
        {
            _socket = socket;
            _tls = tls;
            SupportsMutationReplay = supportsMutationReplay;
            _stopToken = _stop.Token;
            for (var i = 0; i < _pending.Length; i++)
                _ = _slots.Writer.TryWrite(i);
            _reader = ReadAsync();
            _sender = SendAsync();
        }

        internal bool SupportsMutationReplay { get; }

        internal bool IsAvailable => Volatile.Read(ref _aborted) == 0;

        public async ValueTask DisposeAsync()
        {
            Abort();
            await _stop.CancelAsync().ConfigureAwait(false);
            try
            {
#pragma warning disable VSTHRD003 // The socket tasks use ConfigureAwait(false) and have already been unblocked by Abort.
                await Task.WhenAll(_reader, _sender).ConfigureAwait(false);
#pragma warning restore VSTHRD003
            }
            finally
            {
                await _tls.DisposeAsync().ConfigureAwait(false);
                _stop.Dispose();
            }
        }

        internal static async Task<Connection> ConnectAsync(
            Uri endpoint, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, CancellationToken token)
        {
            var (socket, tls, replay) = await BinaryClientProtocol.ConnectAsync(endpoint, provider, tlsOptions, token).ConfigureAwait(false);
            return new Connection(socket, tls, replay);
        }

        internal async ValueTask<IMessage> CallAsync(int control, IMessage request, int bodySize, MessageParser parser, BinaryCallOptions options, bool preventReplay)
        {
            var token = options.CancellationToken;
            int id;
            try
            {
                id = await _slots.Reader.ReadAsync(token).ConfigureAwait(false);
            }
            catch (ChannelClosedException error)
            {
                throw new IOException("The binary cache connection closed.", error);
            }

            var pending = new PendingCall(id, control, request, bodySize, parser, options.Deadline, token);
            Volatile.Write(ref _pending[id], pending);
            if (!_outgoing.Writer.TryWrite(pending))
            {
                _ = Interlocked.Exchange(ref _pending[id], null);
                throw new IOException("The binary cache connection closed.");
            }

            // After a caller cancels, keep the slot until the response is consumed or the connection closes.
            BinaryClientReply result;
            try
            {
                result = await pending.Completion.Task.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!options.CallerToken.IsCancellationRequested)
            {
                // Keep the slot for a late reply while other calls still need this connection.
                if (!HasLiveCalls())
                    Abort();
                if (preventReplay)
                    throw BinaryClientProtocol.UnknownOutcome();
                throw;
            }

            if (result.Response != null)
                return result.Response;
            if (preventReplay && BinaryClientProtocol.IsOutcomeUnconfirmed(result.Failure))
                throw BinaryClientProtocol.UnknownOutcome(result.Failure);
            if (result.Failure != null)
                throw result.Failure;
            throw new IOException("The binary cache connection closed.");
        }

        internal void Abort()
        {
            if (Interlocked.Exchange(ref _aborted, 1) != 0)
                return;

            _ = _outgoing.Writer.TryComplete();
            _ = _slots.Writer.TryComplete();
            _socket.Dispose();
            for (var i = 0; i < _pending.Length; i++)
                _ = Interlocked.Exchange(ref _pending[i], null)?.Completion.TrySetResult(default);
        }

        private static void WriteRequest(Span<byte> buffer, PendingCall item, DateTime now)
        {
            var remaining = Math.Min((item.Deadline - now).TotalMilliseconds, RpcAttemptTimeout.PerAttempt.TotalMilliseconds);
            var timeout = item.CancellationToken.IsCancellationRequested ? 0 : Convert.ToInt32(Math.Max(0, Math.Ceiling(remaining)));
            Frames.WriteMessage(buffer, item.Id, item.BodySize, item.Control | timeout, item.Request);
        }

        private bool HasLiveCalls()
        {
            for (var i = 0; i < _pending.Length; i++)
            {
                if (Volatile.Read(ref _pending[i]) is { CancellationToken.IsCancellationRequested: false })
                    return true;
            }

            return false;
        }

        private async Task SendAsync()
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
            try
            {
                while (await _outgoing.Reader.WaitToReadAsync(_stopToken).ConfigureAwait(false))
                    await SendBatchAsync(buffer).ConfigureAwait(false);
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or InvalidDataException or ObjectDisposedException)
            {
                // Stop the connection and release pending calls in the finally block.
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                Abort();
            }
        }

        private async Task SendBatchAsync(byte[] buffer)
        {
            var size = 0;
            var now = TimeProvider.System.GetUtcNow().UtcDateTime;
            for (var count = 0; count < Frames.MaxBatch && _outgoing.Reader.TryRead(out var item); count++)
            {
                var frameSize = Frames.HeaderBytes + item.BodySize;
                if (size > 0 && size + frameSize > buffer.Length)
                {
                    await _tls.WriteAsync(buffer.AsMemory(0, size), _stopToken).ConfigureAwait(false);
                    size = 0;
                }

                if (frameSize > buffer.Length)
                {
                    await SendLargeAsync(item, now).ConfigureAwait(false);
                    continue;
                }

                WriteRequest(buffer.AsSpan(size), item, now);
                size += frameSize;
            }

            if (size > 0)
                await _tls.WriteAsync(buffer.AsMemory(0, size), _stopToken).ConfigureAwait(false);
        }

        private async Task SendLargeAsync(PendingCall item, DateTime now)
        {
            var length = Frames.HeaderBytes + item.BodySize;
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                WriteRequest(buffer, item, now);
                await _tls.WriteAsync(buffer.AsMemory(0, length), _stopToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private async Task ReadAsync()
        {
            var header = ArrayPool<byte>.Shared.Rent(Frames.HeaderBytes);
            var smallBody = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
            try
            {
                while (IsAvailable)
                {
                    await _tls.ReadExactlyAsync(header.AsMemory(0, Frames.HeaderBytes), _stopToken).ConfigureAwait(false);
                    var (id, size, status) = Frames.Read(header, Frames.MaxResponseBytes);
                    if (id >= _pending.Length || Volatile.Read(ref _pending[id]) is not { } pending)
                        throw new InvalidDataException("Unexpected binary cache reply.");

                    var body = size <= smallBody.Length ? smallBody : ArrayPool<byte>.Shared.Rent(size);
                    try
                    {
                        await _tls.ReadExactlyAsync(body.AsMemory(0, size), _stopToken).ConfigureAwait(false);
                        var reply = BinaryClientProtocol.ParseReply(pending.Parser, status, body.AsSpan(0, size));

                        if (Interlocked.Exchange(ref _pending[id], null) != null)
                        {
                            _ = _slots.Writer.TryWrite(id);
                            _ = pending.Completion.TrySetResult(reply);
                        }
                    }
                    finally
                    {
                        if (!ReferenceEquals(body, smallBody))
                            ArrayPool<byte>.Shared.Return(body);
                    }
                }
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or InvalidDataException or ObjectDisposedException)
            {
                // Stop the connection and release pending calls in the finally block.
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
                ArrayPool<byte>.Shared.Return(smallBody);
                Abort();
            }
        }

        private sealed record PendingCall(int Id, int Control, IMessage Request, int BodySize, MessageParser Parser, DateTime Deadline, CancellationToken CancellationToken)
        {
            internal TaskCompletionSource<BinaryClientReply> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
