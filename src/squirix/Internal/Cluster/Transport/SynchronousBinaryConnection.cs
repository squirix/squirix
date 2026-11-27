using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Multiplexes blocking callers over a binary TLS connection with one reader and one writer.</summary>
internal sealed class SynchronousBinaryConnection : IDisposable
{
    private const string ConnectionClosedMessage = "The binary cache connection closed.";

    private readonly TcpClient _socket;
    private readonly SslStream _tls;
    private readonly SemaphoreSlim _slots = new(128);
    private readonly BlockingCollection<Request> _outgoing = new(new ConcurrentQueue<Request>(), 128);
    private readonly ConcurrentDictionary<int, Pending> _pending = [];
    private readonly bool _supportsMutationReplay;
    private readonly bool _supportsExtendedFrames;
    private readonly Lock _lifetime = new();
    private int _activeCalls;
    private int _next;
    private int _closed;
    private int _users = 3;

    private SynchronousBinaryConnection(TcpClient socket, SslStream tls, bool supportsMutationReplay, bool supportsExtendedFrames)
    {
        _socket = socket;
        _tls = tls;
        _supportsMutationReplay = supportsMutationReplay;
        _supportsExtendedFrames = supportsExtendedFrames;
    }

    internal bool IsAvailable => Volatile.Read(ref _closed) == 0;

    public void Dispose()
    {
        lock (_lifetime)
        {
            if (_closed != 0)
                return;

            Volatile.Write(ref _closed, 1);
        }

        Close();
    }

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The blocking adapter waits for asynchronous credentials only during connection setup.")]
    [SuppressMessage("Usage", "MA0045", Justification = "The blocking adapter waits for connection setup once per physical connection.")]
    internal static SynchronousBinaryConnection Connect(Uri uri, Func<CancellationToken, ValueTask<string>>? provider, SslClientAuthenticationOptions? tlsOptions, CancellationToken token)
    {
        var connecting = SynchronizationContext.Current == null && TaskScheduler.Current == TaskScheduler.Default
            ? BinaryClientProtocol.ConnectAsync(uri, provider, tlsOptions, token)
            : Task.Run(() => BinaryClientProtocol.ConnectAsync(uri, provider, tlsOptions, token), token);
        var (socket, tls, replay, extended) = connecting.GetAwaiter().GetResult();
        var connection = new SynchronousBinaryConnection(socket, tls, replay, extended);
        connection.StartWorkers();
        return connection;
    }

    internal T Send<T>(BinaryOperation operation, WireMessage request, WireParser<T> parser, DateTimeOffset deadline, CancellationToken token, CancellationToken lifetime)
        where T : WireMessage
    {
        EnterCall();
        try
        {
            var prepared = BinaryClientProtocol.PrepareRequest(operation, request, _supportsMutationReplay, _supportsExtendedFrames);
            var reply = SendRequest(operation, in prepared, parser, deadline, token, lifetime);
            return reply as T ?? UnexpectedReply<T>();
        }
        finally
        {
            LeaveCall(token.IsCancellationRequested);
        }
    }

    private static T UnexpectedReply<T>() => throw new InvalidDataException("Unexpected cache response type.");

    private static WireMessage ReadResult(in BinaryClientReply reply, bool preventReplay)
    {
        if (reply.Response is { } response)
            return response;
        if (reply.Failure is not { } failure)
            throw new IOException(ConnectionClosedMessage);

        throw preventReplay && BinaryClientProtocol.IsOutcomeUnconfirmed(failure)
            ? BinaryClientProtocol.UnknownOutcome(failure)
            : failure;
    }

    private static void WriteTimeout(Span<byte> header, DateTimeOffset deadline, DateTimeOffset now)
    {
        var remaining = (deadline - now).TotalMilliseconds;
        var timeout = Convert.ToUInt16(Math.Clamp(Math.Ceiling(remaining), 0, RpcAttemptTimeout.PerAttempt.TotalMilliseconds));
        Frames.WriteRequestTimeout(header, timeout);
    }

    private void Close()
    {
        _outgoing.CompleteAdding();
        _socket.Dispose();
        foreach (var item in _pending)
        {
            if (_pending.TryRemove(item.Key, out var dropped))
                dropped.Complete(default);
        }

        ReleaseUser();
    }

    [SuppressMessage("Usage", "MA0045", Justification = "The caller reserves a slot on the synchronous transport.")]
    private WireMessage SendRequest(BinaryOperation operation, in BinaryClientRequest request, IWireParser parser, DateTimeOffset deadline, CancellationToken token, CancellationToken lifetime)
    {
        _slots.Wait(token);
        var id = Interlocked.Increment(ref _next) & int.MaxValue;
        var pending = new Pending(parser);
        var preventReplay = !_supportsMutationReplay && operation != BinaryOperation.GetDistributed;
        var queued = false;
        try
        {
            Enqueue(new Request(id, request, deadline), pending, token);
            queued = true;
            return ReadResult(pending.Wait(token), preventReplay);
        }
        catch (OperationCanceledException error)
        {
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

    private void Enqueue(in Request request, Pending pending, CancellationToken token)
    {
        if (!_pending.TryAdd(request.Id, pending) || !IsAvailable)
            throw new IOException(ConnectionClosedMessage);

        try
        {
            // The queued request occupies capacity until the writer consumes it, even if its caller has left.
            _outgoing.Add(request, token);
        }
        catch (InvalidOperationException error)
        {
            throw new IOException(ConnectionClosedMessage, error);
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
        lock (_lifetime)
        {
            if (_closed != 0)
                throw new IOException(ConnectionClosedMessage);

            _activeCalls++;
            _ = Interlocked.Increment(ref _users);
        }
    }

    private void LeaveCall(bool timedOut)
    {
        bool retire;
        lock (_lifetime)
        {
            // A caller leaves only after removing its pending reply. Coordinate retirement with new calls.
            _activeCalls--;
            retire = timedOut && _closed == 0 && _activeCalls == 0;
            if (retire)
                Volatile.Write(ref _closed, 1);
        }

        if (retire)
            Close();
        ReleaseUser();
    }

    private void ReadLoop()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
        var ready = new ReadyReply[Frames.MaxBatch];
        var count = 0;
        try
        {
            ReadReplies(buffer, ready, ref count);
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or SocketException or InvalidDataException)
        {
            // Closing the transport wakes every pending caller.
        }
        finally
        {
            ReadyReply.Publish(ready, ref count);
            ArrayPool<byte>.Shared.Return(buffer);
            Dispose();
            ReleaseUser();
        }
    }

    private void ReadReplies(byte[] buffer, ReadyReply[] ready, ref int count)
    {
        while (IsAvailable)
        {
            var read = _tls.Read(buffer);
            if (read == 0)
                break;

            var bytes = buffer.AsSpan(0, read);
            while (!bytes.IsEmpty)
                ReadOneReply(ref bytes, buffer, ready, ref count);

            ReadyReply.Publish(ready, ref count);
        }
    }

    private void ReadOneReply(ref Span<byte> bytes, byte[] buffer, ReadyReply[] ready, ref int count)
    {
        var header = TakeHeader(ref bytes, buffer, Frames.ReplyBytes, ready, ref count);
        var (id, size, status) = Frames.ReadReply(header);
        if (size == Frames.ExtendedSize)
            size = Frames.ReadExtendedSize(TakeHeader(ref bytes, buffer, Frames.ExtendedReplyBytes, ready, ref count));

        _ = _pending.TryGetValue(id, out var pending);
        BinaryClientReply result = default;
        if (size <= bytes.Length)
        {
            if (pending != null)
                result = BinaryClientProtocol.ParseReply(pending.Parser, status, bytes[..size]);
            bytes = bytes[size..];
        }
        else
        {
            ReadyReply.Publish(ready, ref count);
            result = BinaryClientProtocol.ReadBlockingReply(_tls, pending?.Parser, status, size, buffer, bytes);
            bytes = default;
        }

        if (pending != null && _pending.TryRemove(id, out _))
            ready[count++] = new ReadyReply(pending, result);
        if (count == ready.Length)
            ReadyReply.Publish(ready, ref count);
    }

    private Span<byte> TakeHeader(ref Span<byte> bytes, byte[] buffer, int size, ReadyReply[] ready, ref int count)
    {
        if (bytes.Length >= size)
        {
            var header = bytes[..size];
            bytes = bytes[size..];
            return header;
        }

        // A partial header must never hold up the replies from this read.
        ReadyReply.Publish(ready, ref count);
        var available = bytes.Length;
        bytes.CopyTo(buffer);
        bytes = default;
        _tls.ReadExactly(buffer.AsSpan(available, size - available));
        return buffer.AsSpan(0, size);
    }

    private void SendLoop()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
        try
        {
            Span<BufferedFrame> frames = stackalloc BufferedFrame[Frames.MaxBatch];
            foreach (var first in _outgoing.GetConsumingEnumerable(CancellationToken.None))
            {
                var used = 0;
                var count = 0;
                WriteRequest(in first, buffer, frames, ref used, ref count);
                for (var i = 1; i < Frames.MaxBatch && _outgoing.TryTake(out var next); i++)
                    WriteRequest(in next, buffer, frames, ref used, ref count);

                FlushBatch(buffer.AsSpan(0, used), frames[..count]);
            }
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or SocketException)
        {
            // Closing the transport wakes every pending caller.
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            Dispose();
            ReleaseUser();
        }
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

    private void WriteRequest(in Request request, byte[] buffer, Span<BufferedFrame> frames, ref int used, ref int count)
    {
        var length = request.Frame.HeaderBytes + request.Frame.Size;
        if (length > buffer.Length - used && used != 0)
        {
            FlushBatch(buffer.AsSpan(0, used), frames[..count]);
            used = 0;
            count = 0;
        }

        if (length <= buffer.Length)
        {
            request.Frame.WriteTo(buffer.AsSpan(used), request.Id);
            frames[count++] = new BufferedFrame(used, request.Deadline);
            used += length;
            return;
        }

        using var message = new WireSegments();
        var header = request.Frame.WriteTo(message, request.Id);
        WriteTimeout(header.Span, request.Deadline, TimeProvider.System.GetUtcNow());
        foreach (var chunk in message.Chunks())
            _tls.Write(chunk.Span);
    }

    private void FlushBatch(Span<byte> bytes, ReadOnlySpan<BufferedFrame> frames)
    {
        if (frames.IsEmpty)
            return;

        // The earlier flush or serialization may have consumed part of the queued requests' budgets.
        var now = TimeProvider.System.GetUtcNow();
        foreach (ref readonly var frame in frames)
            WriteTimeout(bytes[frame.Offset..], frame.Deadline, now);

        _tls.Write(bytes);
    }

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct BufferedFrame(int Offset, DateTimeOffset Deadline);

    [Immutable]
    private readonly record struct Request(int Id, BinaryClientRequest Frame, DateTimeOffset Deadline);

    private readonly record struct ReadyReply(Pending Call, BinaryClientReply Reply)
    {
        internal static void Publish(ReadyReply[] ready, ref int count)
        {
            for (var i = 0; i < count; i++)
            {
                var item = ready[i];
                item.Call.Complete(item.Reply);
                ready[i] = default;
            }

            count = 0;
        }
    }

    private sealed class Pending
    {
        private readonly object _gate = new();
        private BinaryClientReply _result;
        private bool _completed;

        internal Pending(IWireParser parser)
        {
            Parser = parser;
        }

        internal IWireParser Parser { get; }

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
