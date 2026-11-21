using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Attributes;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Multiplexes blocking callers over a binary TLS connection with one reader and one writer.</summary>
internal sealed class SynchronousBinaryConnection : IDisposable
{
    private readonly TcpClient _socket;
    private readonly SslStream _tls;
    private readonly SemaphoreSlim _slots = new(128);
    private readonly BlockingCollection<Request> _outgoing = new(new ConcurrentQueue<Request>(), 128);
    private readonly ConcurrentDictionary<int, Pending> _pending = [];
    private readonly bool _supportsMutationReplay;
    private int _next;
    private int _closed;
    private int _users = 3;

    private SynchronousBinaryConnection(TcpClient socket, SslStream tls, bool supportsMutationReplay)
    {
        _socket = socket;
        _tls = tls;
        _supportsMutationReplay = supportsMutationReplay;
    }

    internal bool IsAvailable => Volatile.Read(ref _closed) == 0;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        _outgoing.CompleteAdding();
        _socket.Dispose();
        foreach (var item in _pending)
        {
            if (_pending.TryRemove(item.Key, out var dropped))
                dropped.Complete(default);
        }

        ReleaseUser();
    }

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The blocking adapter waits for asynchronous credentials only during connection setup.")]
    [SuppressMessage("Usage", "MA0045", Justification = "The blocking adapter waits for connection setup once per physical connection.")]
    internal static SynchronousBinaryConnection Connect(Uri uri, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, CancellationToken token)
    {
        var connecting = SynchronizationContext.Current == null && TaskScheduler.Current == TaskScheduler.Default
            ? BinaryClientProtocol.ConnectAsync(uri, provider, tlsOptions, token)
            : Task.Run(() => BinaryClientProtocol.ConnectAsync(uri, provider, tlsOptions, token), token);
        var (socket, tls, replay) = connecting.GetAwaiter().GetResult();
        var connection = new SynchronousBinaryConnection(socket, tls, replay);
        connection.StartWorkers();
        return connection;
    }

    internal T Send<T>(BinaryOperation operation, IMessage original, MessageParser<T> parser, DateTimeOffset deadline, CancellationToken token, CancellationToken lifetime)
        where T : class, IMessage<T>
    {
        EnterCall();
        try
        {
            var reply = SendRequest(operation, original, parser, deadline, token, lifetime);
            return reply as T ?? UnexpectedReply<T>();
        }
        finally
        {
            ReleaseUser();
        }
    }

    private static T UnexpectedReply<T>() => throw new InvalidDataException("Unexpected cache response type.");

    private static IMessage ReadResult(in BinaryClientReply reply, bool preventReplay)
    {
        if (reply.Response is { } response)
            return response;
        if (reply.Failure is not { } failure)
            throw new IOException("The binary cache connection closed.");

        throw preventReplay && BinaryClientProtocol.IsOutcomeUnconfirmed(failure)
            ? BinaryClientProtocol.UnknownOutcome(failure)
            : failure;
    }

    [SuppressMessage("Usage", "MA0045", Justification = "The caller reserves a slot on the synchronous transport.")]
    private IMessage SendRequest(BinaryOperation operation, IMessage original, MessageParser parser, DateTimeOffset deadline, CancellationToken token, CancellationToken lifetime)
    {
        _slots.Wait(token);
        var id = Interlocked.Increment(ref _next) & int.MaxValue;
        var pending = new Pending(parser);
        var preventReplay = !_supportsMutationReplay && operation != BinaryOperation.GetDistributed;
        var queued = false;
        try
        {
            Enqueue(operation, original, id, deadline, pending, token);
            queued = true;
            return ReadResult(pending.Wait(token), preventReplay);
        }
        catch (OperationCanceledException error)
        {
            if (_pending.Count <= 1)
                Dispose();
            if (queued && preventReplay && !lifetime.IsCancellationRequested)
                throw BinaryClientProtocol.UnknownOutcome(error);
            throw;
        }
        catch (IOException error) when (queued && preventReplay && !lifetime.IsCancellationRequested)
        {
            throw BinaryClientProtocol.UnknownOutcome(error);
        }
        finally
        {
            _ = _pending.TryRemove(id, out _);
            _ = _slots.Release();
        }
    }

    private void Enqueue(BinaryOperation operation, IMessage original, int id, DateTimeOffset deadline, Pending pending, CancellationToken token)
    {
        var (control, message, size) = BinaryClientProtocol.PrepareRequest(operation, original);
        if (!_pending.TryAdd(id, pending) || !IsAvailable)
            throw new IOException("The binary cache connection closed.");

        try
        {
            if (!_outgoing.TryAdd(new Request(id, control, size, message, deadline), 0, token))
                throw new IOException("The binary synchronous send queue is full.");
        }
        catch (InvalidOperationException error)
        {
            throw new IOException("The binary cache connection closed.", error);
        }
    }

    private void StartWorkers()
    {
        // A connection can outlive the request that opened it.
        if (ExecutionContext.IsFlowSuppressed())
        {
            Start();
            return;
        }

        using var flow = ExecutionContext.SuppressFlow();
        Start();

        void Start()
        {
            _ = Task.Factory.StartNew(ReadLoop, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            _ = Task.Factory.StartNew(SendLoop, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }

    private void EnterCall()
    {
        while (IsAvailable)
        {
            var users = Volatile.Read(ref _users);
            if (users != 0 && Interlocked.CompareExchange(ref _users, users + 1, users) == users)
                return;
        }

        throw new IOException("The binary cache connection closed.");
    }

    private void ReadLoop()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
        try
        {
            while (IsAvailable)
            {
                _tls.ReadExactly(buffer.AsSpan(0, Frames.HeaderBytes));
                var (id, size, status) = Frames.Read(buffer, Frames.MaxResponseBytes);
                var body = size <= buffer.Length ? buffer : ArrayPool<byte>.Shared.Rent(size);
                try
                {
                    _tls.ReadExactly(body.AsSpan(0, size));
                    if (_pending.TryGetValue(id, out var completion))
                    {
                        var result = BinaryClientProtocol.ParseReply(completion.Parser, status, body.AsSpan(0, size));
                        if (_pending.TryRemove(id, out _))
                            completion.Complete(in result);
                    }
                }
                finally
                {
                    if (!ReferenceEquals(body, buffer))
                        ArrayPool<byte>.Shared.Return(body);
                }
            }
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or SocketException or InvalidDataException)
        {
            // Closing the transport wakes every pending caller.
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            FinishLoop();
        }
    }

    private void SendLoop()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
        try
        {
            foreach (var first in _outgoing.GetConsumingEnumerable(CancellationToken.None))
            {
                var used = WriteRequest(in first, buffer, 0);
                for (var i = 1; i < Frames.MaxBatch && _outgoing.TryTake(out var next); i++)
                    used = WriteRequest(in next, buffer, used);

                if (used > 0)
                    _tls.Write(buffer.AsSpan(0, used));
            }
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or SocketException)
        {
            // Closing the transport wakes every pending caller.
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            FinishLoop();
        }
    }

    private void FinishLoop()
    {
        Dispose();
        ReleaseUser();
    }

    [SuppressMessage("Usage", "MA0045", Justification = "The last blocking transport user owns synchronous cleanup.")]
    private void ReleaseUser()
    {
        if (Interlocked.Decrement(ref _users) != 0)
            return;

        _tls.Dispose();
        _slots.Dispose();
        _outgoing.Dispose();
    }

    private int WriteRequest(in Request request, byte[] buffer, int used)
    {
        var length = Frames.HeaderBytes + request.Size;
        var remaining = (request.Deadline - TimeProvider.System.GetUtcNow()).TotalMilliseconds;
        var control = request.Control | Convert.ToInt32(Math.Clamp(Math.Ceiling(remaining), 0, RpcAttemptTimeout.PerAttempt.TotalMilliseconds));
        if (length > buffer.Length - used && used != 0)
        {
            _tls.Write(buffer.AsSpan(0, used));
            used = 0;
        }

        if (length <= buffer.Length)
        {
            Frames.WriteMessage(buffer.AsSpan(used), request.Id, request.Size, control, request.Message);
            return used + length;
        }

        var large = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            Frames.WriteMessage(large, request.Id, request.Size, control, request.Message);
            _tls.Write(large.AsSpan(0, length));
            return 0;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(large);
        }
    }

    [Immutable]
    private readonly record struct Request(int Id, int Control, int Size, IMessage Message, DateTimeOffset Deadline);

    private sealed class Pending
    {
        private readonly object _gate = new();
        private BinaryClientReply _result;
        private bool _completed;

        internal Pending(MessageParser parser)
        {
            Parser = parser;
        }

        internal MessageParser Parser { get; }

        internal void Complete(in BinaryClientReply result)
        {
            lock (_gate)
            {
                _result = result;
                _completed = true;
                Monitor.Pulse(_gate);
            }
        }

        [SuppressMessage("Usage", "MA0045", Justification = "The synchronous caller unregisters after releasing the reply monitor.")]
        internal BinaryClientReply Wait(CancellationToken token)
        {
            using var registration = token.UnsafeRegister(static (state, _) => (state as Pending)?.Wake(), this);
            lock (_gate)
            {
                while (!_completed)
                {
                    token.ThrowIfCancellationRequested();
                    _ = Monitor.Wait(_gate);
                }

                return _result;
            }
        }

        private void Wake()
        {
            lock (_gate)
                Monitor.Pulse(_gate);
        }
    }
}
