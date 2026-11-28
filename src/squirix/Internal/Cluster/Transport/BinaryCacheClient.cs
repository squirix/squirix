using System;
using System.Buffers;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal.Cluster.Transport;

internal sealed class BinaryCacheClient : IAsyncDisposable
{
    private readonly Uri _endpoint;
    private readonly Func<CancellationToken, ValueTask<string>>? _tokenProvider;
    private readonly SslClientAuthenticationOptions? _tlsOptions;
    private readonly Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>>? _invoke;
    private readonly Channel<bool> _connect = Channel.CreateBounded<bool>(1);
    private Connection? _connection;
    private int _disposed;

    internal BinaryCacheClient(
        Uri endpoint,
        Func<CancellationToken, ValueTask<string>>? tokenProvider,
        SslClientAuthenticationOptions? tlsOptions,
        Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>>? invoke = null)
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

    internal ValueTask<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.SetEntry, request, SetAsyncResponse.Parser, in options);

    internal ValueTask<GetEntryAsyncResponse> GetEntryAsync(GetEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetEntry, request, GetEntryAsyncResponse.Parser, in options);

    internal ValueTask<GetValueAsyncResponse> GetValueAsync(GetValueAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetValue, request, GetValueAsyncResponse.Parser, in options);

    internal ValueTask<RemoveAsyncResponse> RemoveAsync(RemoveAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Remove, request, RemoveAsyncResponse.Parser, in options);

    internal ValueTask<TryAddAsyncResponse> AddEntryIfMissingAsync(TryAddEntryAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.TryAddEntry, request, TryAddAsyncResponse.Parser, in options);

    internal ValueTask<TouchAsyncResponse> TouchAsync(TouchAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Touch, request, TouchAsyncResponse.Parser, in options);

    internal ValueTask<RemoveExpirationAsyncResponse> RemoveExpirationAsync(RemoveExpirationAsyncRequest request, in BinaryCallOptions options) => CallAsync(
        BinaryOperation.RemoveExpiration,
        request,
        RemoveExpirationAsyncResponse.Parser,
        in options);

    internal ValueTask<UpdateAsyncResponse> UpdateAsync(UpdateAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.Update, request, UpdateAsyncResponse.Parser, in options);

    internal ValueTask<GetExpirationAsyncResponse> GetExpirationAsync(GetExpirationAsyncRequest request, in BinaryCallOptions options) => CallAsync(
        BinaryOperation.GetExpiration,
        request,
        GetExpirationAsyncResponse.Parser,
        in options);

    internal ValueTask<GetOrAddAsyncResponse> GetOrAddAsync(GetOrAddAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.GetOrAdd, request, GetOrAddAsyncResponse.Parser, in options);

    internal ValueTask<SetAsyncResponse> SetDistributedAsync(SetDistributedAsyncRequest request, in BinaryCallOptions options) =>
        CallAsync(BinaryOperation.SetDistributed, request, SetAsyncResponse.Parser, in options);

    internal ValueTask<GetDistributedAsyncResponse> GetDistributedAsync(GetDistributedAsyncRequest request, in BinaryCallOptions options) => CallAsync(
        BinaryOperation.GetDistributed,
        request,
        GetDistributedAsyncResponse.Parser,
        in options);

    internal ValueTask<RefreshDistributedAsyncResponse> RefreshDistributedAsync(RefreshDistributedAsyncRequest request, in BinaryCallOptions options) => CallAsync(
        BinaryOperation.RefreshDistributed,
        request,
        RefreshDistributedAsyncResponse.Parser,
        in options);

    private static T ThrowUnexpectedResponse<T>() => throw new InvalidDataException("Unexpected cache response type.");

    private ValueTask<T> CallAsync<T>(BinaryOperation operation, WireMessage request, WireParser<T> parser, in BinaryCallOptions options)
        where T : WireMessage =>
        Volatile.Read(ref _disposed) == 0 && _invoke == null && Volatile.Read(ref _connection) is { IsAvailable: true } ready
            ? ready.SubmitOperationAsync(operation, request, parser, in options)
            : ConnectAndCallAsync(operation, request, parser, options);

    private async ValueTask<T> ConnectAndCallAsync<T>(BinaryOperation operation, WireMessage request, WireParser<T> parser, BinaryCallOptions options)
        where T : WireMessage
    {
        using var deadline = BinaryCallAlarm.Materialize(in options);
        if (deadline != null)
            options = options with { Expires = 0, CancellationToken = deadline.Token };
        if (_invoke != null)
        {
            _ = BinaryClientProtocol.PrepareMutationIdentity(request, true);
            var response = await _invoke(operation, request, parser, options).ConfigureAwait(false);
            return response as T ?? ThrowUnexpectedResponse<T>();
        }

        Connection? connection = null;
        try
        {
            connection = await GetConnectionAsync(options.CancellationToken).ConfigureAwait(false);
            return await connection.SubmitOperationAsync(operation, request, parser, in options).ConfigureAwait(false);
        }
        catch (Exception error) when (error is AuthenticationException or ConnectionRejectedException)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unauthenticated, error.Message, error));
        }
        catch (OperationCanceledException error) when (connection == null && !options.CallerToken.IsCancellationRequested)
        {
            throw new SquirixTransportException(
                new CallStatus(CacheTransportStatusCode.Unavailable, "Unable to establish the binary connection within the attempt budget.", error))
            {
                ConnectAttemptTimedOut = deadline?.IsCancellationRequested == true,
            };
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
        private const int PendingCapacity = BinaryRequestSlots.Capacity;

        private readonly TcpClient _socket;
        private readonly SslStream _tls;
        private readonly CancellationTokenSource _stop = new();
        private readonly CancellationToken _stopToken;
        private readonly BinaryRequestSlots _slots = new();

        /// <summary>Every published request owns a correlation slot until its reply arrives.</summary>
        private readonly Channel<Outgoing> _outgoing = Channel.CreateUnbounded<Outgoing>(new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = true });
        private readonly byte[]?[] _prefixes = new byte[PendingCapacity][];
        private readonly IPendingCall?[] _reusable = new IPendingCall?[PendingCapacity];
        private readonly ReadyReplies _ready = new();
        private readonly IPendingCall?[] _pending = new IPendingCall?[PendingCapacity];
        private readonly BinaryCallAlarm _alarm;
        private readonly Task _reader;
        private readonly Task _sender;
        private volatile bool _sending;
        private BinaryConnectionLifetime _lifetime;

        private Connection(TcpClient socket, SslStream tls, bool supportsMutationReplay, bool supportsExtendedFrames)
        {
            _lifetime = default;
            _socket = socket;
            _tls = tls;
            SupportsMutationReplay = supportsMutationReplay;
            SupportsExtendedFrames = supportsExtendedFrames;
            _stopToken = _stop.Token;
            _alarm = new BinaryCallAlarm(_pending);
            _reader = ReadAsync();
            _sender = SendAsync();
        }

        private interface IPendingCall : IBinaryDeadline
        {
            int Id { get; }
            IWireParser Parser { get; }
            DateTime Deadline { get; }
            CancellationToken CancellationToken { get; }

            bool BodyReady { get; }

            void MarkBodyReady();

            void Reject(Exception error);

            void Complete(in BinaryClientReply reply);
        }

        internal bool SupportsMutationReplay { get; }

        internal bool SupportsExtendedFrames { get; }

        internal bool IsAvailable => _lifetime.IsOpen;

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
            Uri endpoint,
            Func<CancellationToken, ValueTask<string>>? provider,
            SslClientAuthenticationOptions? tlsOptions,
            CancellationToken token)
        {
            var (socket, tls, replay, extended) = await BinaryClientProtocol.ConnectAsync(endpoint, provider, tlsOptions, token).ConfigureAwait(false);
            return new Connection(socket, tls, replay, extended);
        }

        internal ValueTask<T> SubmitOperationAsync<T>(BinaryOperation operation, WireMessage message, WireParser<T> parser, in BinaryCallOptions options)
            where T : WireMessage
        {
            var available = _slots.TryTake(out var id);
            var submitted = false;
            BinaryClientRequest request;
            try
            {
                var senderBusy = _sending;
                var capture = senderBusy && available ? _prefixes[id] ??= new byte[128] : null;
                request = available && !senderBusy
                    ? BinaryClientProtocol.Capture(operation, message, SupportsMutationReplay)
                    : BinaryClientProtocol.PrepareRequest(operation, message, SupportsMutationReplay, SupportsExtendedFrames, capture);
                if (BinaryCallAlarm.HasExpired(options.Expires) || options.CancellationToken.IsCancellationRequested)
                    return ValueTask.FromCanceled<T>(ValidateCanceledRequest(in request, options.CancellationToken));
                submitted = true;
            }
            catch (Exception error) when (error is IOException or SocketException or InvalidDataException)
            {
                Abort();
                return ValueTask.FromException<T>(Unavailable(error));
            }
            catch (Exception error) when (error is SquirixTransportException or ArgumentException or OverflowException)
            {
                return ValueTask.FromException<T>(error);
            }
            finally
            {
                if (available && !submitted)
                    _slots.Release(id);
            }

            var preventReplay = !SupportsMutationReplay &&
                                operation is not (BinaryOperation.GetEntry or BinaryOperation.GetValue or BinaryOperation.GetExpiration or BinaryOperation.GetDistributed);
            return available
                ? SubmitAsync(id, in request, parser, in options, preventReplay)
                : WaitForSlotAndSendAsync(request, parser, options, preventReplay);
        }

        internal void Abort()
        {
            if (_lifetime.TryClose())
                CloseConnection();
        }

        private static SquirixTransportException Unavailable(Exception? error = null) =>
            new(new CallStatus(CacheTransportStatusCode.Unavailable, "Binary connection closed.", error));

        private static int RemainingMilliseconds(IPendingCall item, DateTime now)
        {
            var remaining = Math.Min((item.Deadline - now).TotalMilliseconds, RpcAttemptTimeout.PerAttempt.TotalMilliseconds);
            return item.CancellationToken.IsCancellationRequested || BinaryCallAlarm.HasExpired(item.Expires) ? 0 : Convert.ToInt32(Math.Max(0, Math.Ceiling(remaining)));
        }

        private CancellationToken ValidateCanceledRequest(in BinaryClientRequest request, CancellationToken token)
        {
            if (request.Size < 0)
                _ = request.Measure(SupportsExtendedFrames, default);
            return token.IsCancellationRequested ? token : new CancellationToken(true);
        }

        private void CloseConnection()
        {
            _ = _outgoing.Writer.TryComplete();
            _slots.Close();
            _alarm.Dispose();
            _socket.Dispose();
            for (var i = 0; i < _pending.Length; i++)
                Interlocked.Exchange(ref _pending[i], null)?.Complete(default);
        }

        private async ValueTask<T> WaitForSlotAndSendAsync<T>(BinaryClientRequest request, WireParser<T> parser, BinaryCallOptions options, bool preventReplay)
            where T : WireMessage
        {
            using var deadline = BinaryCallAlarm.Materialize(in options);
            if (deadline != null)
                options = options with { Expires = 0, CancellationToken = deadline.Token };
            int id;
            try
            {
                id = await _slots.WaitAsync(options.CancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException error)
            {
                throw Unavailable(error);
            }

            return await SubmitAsync(id, in request, parser, in options, preventReplay).ConfigureAwait(false);
        }

        private ValueTask<T> SubmitAsync<T>(int id, in BinaryClientRequest request, WireParser<T> parser, in BinaryCallOptions options, bool preventReplay)
            where T : WireMessage
        {
            if (!_lifetime.TryEnter())
            {
                _slots.Release(id);
                return ValueTask.FromException<T>(Unavailable());
            }

            var pending = options.CancellationToken.CanBeCanceled
                ? new CancellableCall<T>(this, id, parser, in options, preventReplay)
                : PendingCall<T>.Rent(this, id, parser, in options, preventReplay);
            if (preventReplay && request.Size < 0)
                pending.UnmeasuredMutation = request.Message;
            var operation = pending.Operation;
            Volatile.Write(ref _pending[id], pending);
            if (!_outgoing.Writer.TryWrite(new Outgoing(pending, request)))
            {
                if (Interlocked.Exchange(ref _pending[id], null) != null)
                    pending.Reject(Unavailable());
                return operation;
            }

            pending.WatchCancellation();
            if (options.Expires != 0)
                _alarm.Watch(options.Expires);
            return operation;
        }

        private async Task SendAsync()
        {
            byte[]? buffer = null;
            try
            {
                while (true)
                {
                    var ready = _outgoing.Reader.WaitToReadAsync(CancellationToken.None);
                    var handoff = !ready.IsCompletedSuccessfully;
                    if (!await ready.ConfigureAwait(false))
                        return;
                    if (handoff)
                        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
                    _sending = true;
                    buffer ??= ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
                    await SendBatchAsync(buffer).ConfigureAwait(false);
                    _sending = false;
                }
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or InvalidDataException or ObjectDisposedException)
            {
                // Stop the connection and release pending calls in the finally block.
            }
            finally
            {
                if (buffer != null)
                    ArrayPool<byte>.Shared.Return(buffer, true);
                Abort();
            }
        }

        private async Task SendBatchAsync(byte[] buffer)
        {
            var size = 0;
            var now = TimeProvider.System.GetUtcNow().UtcDateTime;
            for (var count = 0; count < PendingCapacity && _outgoing.Reader.TryRead(out var outgoing); count++)
            {
                var item = outgoing.Call;
                if (!PrepareFrame(in outgoing, buffer.AsSpan(size), out var request, out var filled))
                    continue;

                var frameSize = request.HeaderBytes + request.Size;
                if (size > 0 && size + frameSize > buffer.Length)
                {
                    await _tls.WriteAsync(buffer.AsMemory(0, size), _stopToken).ConfigureAwait(false);
                    size = 0;
                }

                if (frameSize > buffer.Length)
                {
                    await SendLargeAsync(item, request, now, buffer).ConfigureAwait(false);
                    continue;
                }

                var timeout = RemainingMilliseconds(item, now);
                if (filled)
                    request.WriteHeader(buffer.AsSpan(size), item.Id, timeout);
                else
                    request.WriteTo(buffer.AsSpan(size), item.Id, timeout);
                item.MarkBodyReady();
                size += frameSize;
            }

            if (size > 0)
                await _tls.WriteAsync(buffer.AsMemory(0, size), _stopToken).ConfigureAwait(false);
        }

        private bool PrepareFrame(in Outgoing outgoing, Span<byte> target, out BinaryClientRequest request, out bool filled)
        {
            var item = outgoing.Call;
            request = outgoing.Request;
            filled = false;
            if (request.Size >= 0)
                return true;
            var body = target.Length > Frames.RequestBytes ? target[Frames.RequestBytes..] : default;
            try
            {
                request = request.Measure(SupportsExtendedFrames, body);
                filled = !body.IsEmpty && request.Size <= body.Length;
                return true;
            }
            catch (Exception error) when (error is SquirixTransportException or ArgumentException or OverflowException)
            {
                if (Interlocked.Exchange(ref _pending[item.Id], null) != null)
                {
                    _slots.Release(item.Id);
                    item.Reject(error);
                }

                return false;
            }
        }

        private async Task SendLargeAsync(IPendingCall item, BinaryClientRequest request, DateTime now, byte[] buffer)
        {
            using var message = new WireSegments(buffer);
            _ = request.WriteTo(message, item.Id, RemainingMilliseconds(item, now));
            item.MarkBodyReady();
            await message.SendAsync(_tls, _stopToken).ConfigureAwait(false);
        }

        private async Task ReadAsync()
        {
            byte[]? buffer = null;
            try
            {
                // SslStream can wait for input before the session rents its first reply buffer.
                _ = await _tls.ReadAsync(Memory<byte>.Empty, _stopToken).ConfigureAwait(false);
                buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
                var buffered = 0;
                while (IsAvailable)
                {
                    var read = await _tls.ReadAsync(buffer.AsMemory(buffered), _stopToken).ConfigureAwait(false);
                    if (read == 0)
                        return;
                    var available = buffered + read;
                    var consumed = await ConsumeRepliesAsync(buffer, available).ConfigureAwait(false);
                    buffered = available - consumed;
                    if (consumed != 0 && buffered != 0)
                        buffer.AsSpan(consumed, buffered).CopyTo(buffer);
                }
            }
            catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or InvalidDataException or ObjectDisposedException)
            {
                // Stop the connection and release pending calls in the finally block.
            }
            finally
            {
                if (buffer != null)
                    ArrayPool<byte>.Shared.Return(buffer);
                Abort();
            }
        }

        private async ValueTask<int> ConsumeRepliesAsync(byte[] buffer, int available)
        {
            var offset = 0;
            while (available - offset >= Frames.ReplyBytes)
            {
                var (id, size, status) = Frames.ReadReply(buffer.AsSpan(offset));
                var header = Frames.ReplyBytes;
                if (size == Frames.ExtendedSize)
                {
                    header += Frames.ExtendedReplyBytes;
                    if (available - offset < header)
                        break;
                    size = Frames.ReadExtendedSize(buffer.AsSpan(offset + Frames.ReplyBytes));
                }

                if (id >= _pending.Length || Volatile.Read(ref _pending[id]) is not { BodyReady: true } pending)
                    throw new InvalidDataException("Unexpected binary cache reply.");

                var bodyOffset = offset + header;
                if (available - bodyOffset >= size)
                {
                    var reply = BinaryClientProtocol.ParseReply(pending.Parser, status, buffer.AsSpan(bodyOffset, size));
                    PublishReply(id, pending, in reply);
                    offset = bodyOffset + size;
                    continue;
                }

                if (size <= buffer.Length - header)
                    break;

                var prefix = buffer.AsMemory(bodyOffset, available - bodyOffset);
                var large = await BinaryClientProtocol.ReadPartialReplyAsync(_tls, pending.Parser, status, size, buffer, prefix, _stopToken).ConfigureAwait(false);
                PublishReply(id, pending, in large);
                return available;
            }

            return offset;
        }

        private void PublishReply(int id, IPendingCall pending, in BinaryClientReply reply)
        {
            if (Interlocked.Exchange(ref _pending[id], null) == null)
                return;

            _slots.Release(id);
            pending.Complete(in reply);
        }

        private readonly record struct Outgoing(IPendingCall Call, BinaryClientRequest Request);

        private sealed class ReadyReplies : IThreadPoolWorkItem
        {
            private readonly System.Collections.Concurrent.ConcurrentQueue<IThreadPoolWorkItem> _queue = new();
            private int _scheduled;

            internal object?[] Slots { get; } = new object[PendingCapacity];

            public void Execute()
            {
                Volatile.Write(ref _scheduled, 0);
                while (_queue.TryDequeue(out var ready))
                {
                    // Another worker can deliver remaining replies if this continuation blocks.
                    if (!_queue.IsEmpty)
                        EnsureWorker();
                    ready.Execute();
                }
            }

            internal void Publish(IThreadPoolWorkItem ready)
            {
                _queue.Enqueue(ready);
                EnsureWorker();
            }

            private void EnsureWorker()
            {
                if (Volatile.Read(ref _scheduled) == 0 && Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
                    _ = ThreadPool.UnsafeQueueUserWorkItem(this, false);
            }
        }

        private sealed class ReplySource<T> : IValueTaskSource<T>, IThreadPoolWorkItem
            where T : WireMessage
        {
            private readonly ReadyReplies _ready;
            private readonly int _id;
            private ManualResetValueTaskSourceCore<T> _result;
            private Action<object?>? _continuation;
            private object? _continuationState;

            internal ReplySource(ReadyReplies ready, int id)
            {
                _ready = ready;
                _id = id;
            }

            internal ValueTask<T> Operation => new(this, _result.Version);

            public ValueTaskSourceStatus GetStatus(short token) => _result.GetStatus(token);

            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                if (flags != ValueTaskSourceOnCompletedFlags.None || _result.GetStatus(token) != ValueTaskSourceStatus.Pending)
                {
                    _result.RunContinuationsAsynchronously = true;
                    _result.OnCompleted(continuation, state, token, flags);
                    return;
                }

                _continuation = continuation;
                _continuationState = state;
                _result.OnCompleted(QueueContinuation, this, token, flags);
            }

            public T GetResult(short token)
            {
                if (_result.GetStatus(token) == ValueTaskSourceStatus.Pending)
                    throw new InvalidOperationException("The reply has not arrived.");
                try
                {
                    return _result.GetResult(token);
                }
                finally
                {
                    _result.Reset();
                    _result.RunContinuationsAsynchronously = false;
                    _ = Interlocked.CompareExchange(ref _ready.Slots[_id], this, null);
                }
            }

            void IThreadPoolWorkItem.Execute()
            {
                var continuation = _continuation!;
                var state = _continuationState;
                _continuation = null;
                _continuationState = null;
                continuation(state);
            }

            internal void SetResult(T value) => _result.SetResult(value);

            internal void SetException(Exception error) => _result.SetException(error);

            private static void QueueContinuation(object? item)
            {
                if (item is ReplySource<T> pending)
                    pending._ready.Publish(pending);
            }
        }

        private class PendingCall<T> : IPendingCall
            where T : WireMessage
        {
            private readonly Connection _connection;
            private BinaryCallOptions _options;
            private bool _preventReplay;
            private ReplySource<T>? _result;
            private volatile bool _bodyReady;
            private BinaryCallLifetime _lifetime;

            internal PendingCall(Connection connection, int id, IWireParser parser, in BinaryCallOptions options, bool preventReplay)
            {
                _connection = connection;
                _result = Interlocked.Exchange(ref connection._ready.Slots[id], null) as ReplySource<T> ?? new ReplySource<T>(connection._ready, id);
                Id = id;
                Parser = parser;
                _options = options;
                _preventReplay = preventReplay;
                _lifetime = new BinaryCallLifetime(options.Expires);
            }

            public int Id { get; }
            public IWireParser Parser { get; private set; }
            public DateTime Deadline => _options.Deadline;
            public CancellationToken CancellationToken => _options.CancellationToken;

            public bool BodyReady => _bodyReady;

            public long Expires => _lifetime.Expires;

            public virtual bool IsLive => !IsCompleted;

            internal ValueTask<T> Operation => Result.Operation;

            internal WireMessage? UnmeasuredMutation { get; set; }

            protected bool IsCompleted => _lifetime.IsCompleted;

            private ReplySource<T> Result => _result ?? ThrowCompleted();

            public void MarkBodyReady()
            {
                UnmeasuredMutation = null;
                _bodyReady = true;
            }

            public void Complete(in BinaryClientReply reply)
            {
                if (!_lifetime.TryComplete())
                    return;
                _connection._lifetime.Leave();
                ReleaseRegistration();
                if (reply.Response is T response)
                {
                    Result.SetResult(response);
                    if (!_options.CancellationToken.CanBeCanceled)
                    {
                        _options = default;
                        _result = null;
                        _ = Interlocked.CompareExchange(ref _connection._reusable[Id], this, null);
                    }
                }
                else if (_preventReplay && BinaryClientProtocol.IsOutcomeUnconfirmed(reply.Failure))
                {
                    Result.SetException(BinaryClientProtocol.UnknownOutcome(reply.Failure));
                }
                else
                {
                    Result.SetException(reply.Failure ?? Unavailable());
                }
            }

            public void Reject(Exception error)
            {
                if (_lifetime.TryComplete())
                {
                    _connection._lifetime.Leave();
                    ReleaseRegistration();
                    Result.SetException(error);
                }
            }

            public void Expire(long observed)
            {
                if (_lifetime.TryExpire(observed))
                    CancelClaimed();
            }

            internal static PendingCall<T> Rent(Connection connection, int id, IWireParser parser, in BinaryCallOptions options, bool preventReplay)
            {
                if (Interlocked.Exchange(ref connection._reusable[id], null) is not PendingCall<T> item)
                    return new PendingCall<T>(connection, id, parser, in options, preventReplay);
                item._options = options;
                item.Parser = parser;
                item._preventReplay = preventReplay;
                item._bodyReady = false;
                item._result = Interlocked.Exchange(ref connection._ready.Slots[id], null) as ReplySource<T> ?? new ReplySource<T>(connection._ready, id);
                item._lifetime.Start(options.Expires);
                return item;
            }

            internal virtual void WatchCancellation()
            {
            }

            protected virtual void ReleaseRegistration()
            {
            }

            protected void Cancel()
            {
                if (!_lifetime.TryComplete())
                    return;
                CancelClaimed();
            }

            private static ReplySource<T> ThrowCompleted() => throw new InvalidOperationException("The request has completed.");

            private void CancelClaimed()
            {
                var timedOut = !_options.CallerToken.IsCancellationRequested;
                var token = timedOut && _options.Expires != 0 ? new CancellationToken(true) : CancellationToken;
                ReleaseRegistration();
                var validationError = timedOut ? ValidateUnsentMutation() : null;
                _connection._lifetime.Leave();
                var close = timedOut && validationError == null && _connection._lifetime.TryCloseIfIdle();
                Result.SetException(validationError ?? (timedOut && _preventReplay ? BinaryClientProtocol.UnknownOutcome() : new OperationCanceledException(token)));
                if (close)
                    _connection.CloseConnection();
            }

            private Exception? ValidateUnsentMutation()
            {
                if (UnmeasuredMutation is not { } message)
                    return null;
                try
                {
                    // A single-copy mutation omits its operation ID. Size-only validation never reads the payload bytes.
                    var request = new BinaryClientRequest(0, message, -1, string.Empty, 0, false);
                    _ = request.Measure(_connection.SupportsExtendedFrames, default);
                    return null;
                }
                catch (Exception error) when (error is SquirixTransportException or ArgumentException or OverflowException)
                {
                    return error;
                }
            }
        }

        private sealed class CancellableCall<T> : PendingCall<T>
            where T : WireMessage
        {
            private CancellationTokenRegistration _registration;
            private int _registrationState;

            internal CancellableCall(Connection connection, int id, IWireParser parser, in BinaryCallOptions options, bool preventReplay)
                : base(connection, id, parser, in options, preventReplay)
            {
            }

            public override bool IsLive => base.IsLive && !CancellationToken.IsCancellationRequested;

            internal override void WatchCancellation()
            {
                if (IsCompleted)
                    return;

                _registration = CancellationToken.UnsafeRegister(
                    static state =>
                    {
                        if (state is CancellableCall<T> pending)
                            pending.Cancel();
                    },
                    this);

                // If the reply won during registration, this thread owns its cleanup.
                if (Interlocked.CompareExchange(ref _registrationState, 1, 0) == 2)
                    _ = _registration.Unregister();
            }

            protected override void ReleaseRegistration()
            {
                if (Interlocked.Exchange(ref _registrationState, 2) == 1)
                    _ = _registration.Unregister();
            }
        }
    }
}
