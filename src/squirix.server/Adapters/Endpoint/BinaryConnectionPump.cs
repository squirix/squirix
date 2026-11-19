using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Dispatches bounded concurrent requests and flushes ready responses from one writer.</summary>
internal sealed partial class BinaryConnectionPump
{
    private const int MaxPending = 16;
    private const int DispatchBatchLimit = 4;

    /// <summary>The maximum number of ready replies staged before publishing a group.</summary>
    /// <remarks>Ready replies can wait for synchronous work on later buffered calls. The reader publishes before waiting for input or a slot.</remarks>
    private const int ReadyBatchLimit = 8;

    private readonly ConnectionContext _connection;
    private readonly ILogger _log;
    private readonly TimeProvider _timeProvider;
    private readonly Func<(int Id, int Size, int Control), ReadOnlyMemory<byte>, ValueTask<BinaryResponse>> _process;
    private readonly Task[] _jobs = new Task[MaxPending];
    private readonly Channel<int> _slots = Channel.CreateBounded<int>(MaxPending);
    private readonly Channel<ResponseBatch> _outgoing = Channel.CreateBounded<ResponseBatch>(
        new BoundedChannelOptions(MaxPending) { SingleReader = true });

    private ResponseBatch _ready;

    internal BinaryConnectionPump(
        ConnectionContext connection,
        Func<(int Id, int Size, int Control), ReadOnlyMemory<byte>, ValueTask<BinaryResponse>> process,
        ILogger log,
        TimeProvider timeProvider)
    {
        _connection = connection;
        _log = log;
        _timeProvider = timeProvider;
        _process = process;
        for (var i = 0; i < _jobs.Length; i++)
        {
            _jobs[i] = Task.CompletedTask;
            _ = _slots.Writer.TryWrite(i);
        }
    }

    internal async Task RunAsync(CancellationToken readStop)
    {
        var writer = WriteAsync();
        try
        {
            await ReadAsync(readStop).ConfigureAwait(false);
        }
        finally
        {
            await CompleteAsync(writer).ConfigureAwait(false);
        }
    }

    [LoggerMessage(EventId = 3032, Level = LogLevel.Error, Message = "Binary cache request {RequestId} failed")]
    private static partial void LogCallFailure(ILogger logger, int requestId, Exception exception);

    private static async ValueTask<byte[]> ReadBodyAsync(Stream input, int size, CancellationToken cancellationToken)
    {
        var body = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            await input.ReadExactlyAsync(body.AsMemory(0, size), cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return body;
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(body);
            throw;
        }
    }

    private static int WriteResponse(PipeWriter writer, in PendingResponse frame)
    {
        var payload = frame.Response.Payload;
        var status = frame.Response.Status;
        var size = payload?.CalculateSize() ?? 0;
        if (size > Frames.MaxResponseBytes)
        {
            size = 0;
            status = FrameStatus.ResourceExhausted;
        }

        var length = Frames.HeaderBytes + size;
        var target = writer.GetSpan(length);
        if (size == 0 || payload == null)
            Frames.Write(target, frame.Id, 0, Unsafe.BitCast<FrameStatus, int>(status));
        else
            Frames.WriteMessage(target, frame.Id, size, Unsafe.BitCast<FrameStatus, int>(status), payload);

        writer.Advance(length);
        return length;
    }

    private static (int Id, int Size, int Control) ReadHeader(in ReadOnlySequence<byte> buffer)
    {
        var first = buffer.FirstSpan;
        if (first.Length >= Frames.HeaderBytes)
            return Frames.Read(first, Frames.MaxRequestBytes);

        Span<byte> header = stackalloc byte[Frames.HeaderBytes];
        buffer.Slice(0, Frames.HeaderBytes).CopyTo(header);
        return Frames.Read(header, Frames.MaxRequestBytes);
    }

    private async Task ReadAsync(CancellationToken stop)
    {
        var input = _connection.Transport.Input;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var slots = _slots.Reader.WaitToReadAsync(stop);
                PublishBeforeWait(slots.IsCompletedSuccessfully, ref _ready);
                if (!await slots.ConfigureAwait(false))
                    return;

                var read = input.ReadAsync(stop);
                PublishBeforeWait(read.IsCompletedSuccessfully, ref _ready);
                var result = await read.ConfigureAwait(false);
                var partial = await ConsumeBufferedAsync(input, result, stop).ConfigureAwait(false);
                if (partial is { } frame && !await ReadPartialAsync(input, frame, stop).ConfigureAwait(false))
                    return;
            }
        }
        catch (IOException)
        {
            _connection.Abort();
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            // Allow already accepted calls to complete when Kestrel requests shutdown.
        }
        finally
        {
            Publish(ref _ready);
        }
    }

    private async ValueTask<PartialFrame?> ConsumeBufferedAsync(PipeReader input, ReadResult result, CancellationToken stop)
    {
        var buffer = result.Buffer;
        var consumed = buffer.Start;
        var examined = consumed;
        QueuedCall[]? queued = null;
        var queuedCount = 0;
        try
        {
            if (result.IsCanceled || stop.IsCancellationRequested)
            {
                examined = buffer.End;
                stop.ThrowIfCancellationRequested();
                throw new OperationCanceledException("The binary input pipe canceled a read.");
            }

            while (buffer.Length >= Frames.HeaderBytes && !stop.IsCancellationRequested)
            {
                if (!_slots.Reader.TryRead(out var slot))
                    return null;

                var header = buffer;
                buffer = buffer.Slice(Frames.HeaderBytes);
                consumed = examined = buffer.Start;
                var frame = ReadHeader(in header);
                if (buffer.Length < frame.Size)
                    return new PartialFrame(slot, frame);

                var body = ArrayPool<byte>.Shared.Rent(frame.Size);
                buffer.Slice(0, frame.Size).CopyTo(body);
                buffer = buffer.Slice(frame.Size);
                consumed = examined = buffer.Start;
                if (stop.IsCancellationRequested)
                {
                    ArrayPool<byte>.Shared.Return(body);
                    break;
                }

                await DispatchBufferedAsync(new QueuedCall(slot, frame, body), ref queued, ref queuedCount).ConfigureAwait(false);
            }

            examined = buffer.End;
            stop.ThrowIfCancellationRequested();

            // Detect EOF after yielding to the same slot gate as the next request.
            return result.IsCompleted && buffer.Length == result.Buffer.Length ? throw new EndOfStreamException() : null;
        }
        finally
        {
            input.AdvanceTo(consumed, examined);
            QueueExecution(ref queued, ref queuedCount);
        }
    }

    private ValueTask DispatchBufferedAsync(in QueuedCall call, ref QueuedCall[]? queued, ref int queuedCount)
    {
        var operation = Unsafe.BitCast<int, BinaryOperation>(Frames.ReadOperationId(call.Frame.Control));
        if (operation is not (BinaryOperation.GetDistributed or BinaryOperation.GetValue))
        {
            QueueExecution(ref queued, ref queuedCount);
            return DispatchAsync(call.Slot, call.Frame, call.Body);
        }

        queued ??= ArrayPool<QueuedCall>.Shared.Rent(DispatchBatchLimit);
        queued[queuedCount++] = call;
        if (queuedCount == DispatchBatchLimit)
            QueueExecution(ref queued, ref queuedCount);

        return ValueTask.CompletedTask;
    }

    private void QueueExecution(ref QueuedCall[]? queued, ref int queuedCount)
    {
        if (queued is not { } batch)
            return;

        var count = queuedCount;
        queued = null;
        queuedCount = 0;
        Span<int> slots = stackalloc int[DispatchBatchLimit];
        for (var i = 0; i < count; i++)
            slots[i] = batch[i].Slot;

        var job = Task.Factory.StartNew(
            static state => state is QueuedDispatch work
                ? work.Pump.ExecuteQueuedAsync(work.Batch, work.Count)
                : throw new InvalidOperationException("Missing queued binary dispatch."),
            new QueuedDispatch(this, batch, count),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default).Unwrap();
        for (var i = 0; i < count; i++)
            _jobs[slots[i]] = job;
    }

    private async Task ExecuteQueuedAsync(QueuedCall[] batch, int count)
    {
        var ready = default(ResponseBatch);
        List<Task>? incomplete = null;
        try
        {
            for (var i = 0; i < count; i++)
            {
                var call = batch[i];
                if (_connection.ConnectionClosed.IsCancellationRequested)
                {
                    ArrayPool<byte>.Shared.Return(call.Body);
                    continue;
                }

                var pending = ExecuteCallAsync(call.Slot, call.Frame, call.Body);
                if (!pending.IsCompletedSuccessfully)
                    (incomplete ??= []).Add(PublishOnCompletionAsync(pending));
                else if (await pending.ConfigureAwait(false) is { } response)
                    Stage(in response, ref ready);
            }

            Publish(ref ready);
            if (incomplete != null)
                await Task.WhenAll(incomplete).ConfigureAwait(false);
        }
        finally
        {
            ready.Release();
            ArrayPool<QueuedCall>.Shared.Return(batch, true);
        }
    }

    private async ValueTask<bool> ReadPartialAsync(PipeReader input, PartialFrame partial, CancellationToken stop)
    {
        var stream = input.AsStream(true);
        await using var streamLifetime = stream.ConfigureAwait(false);
        byte[] body;
        using (var deadline = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, _timeProvider, stop))
        {
            try
            {
                var bodyRead = ReadBodyAsync(stream, partial.Frame.Size, deadline.Token);
                PublishBeforeWait(bodyRead.IsCompletedSuccessfully, ref _ready);
                body = await bodyRead.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !stop.IsCancellationRequested)
            {
                // A timed-out body leaves the connection between frame boundaries.
                _connection.Abort();
                return false;
            }
        }

        await DispatchAsync(partial.Slot, partial.Frame, body).ConfigureAwait(false);
        return true;
    }

    private async ValueTask DispatchAsync(int slot, (int Id, int Size, int Control) frame, byte[] body)
    {
        var pending = ExecuteCallAsync(slot, frame, body);
        if (!pending.IsCompletedSuccessfully)
        {
            _jobs[slot] = PublishOnCompletionAsync(pending);
            return;
        }

        _jobs[slot] = Task.CompletedTask;
        if (await pending.ConfigureAwait(false) is { } response)
            Stage(in response, ref _ready);
    }

    private bool TryReadResponse(ref ResponseBatch current, ref int index, out PendingResponse response)
    {
        if (current.Count == 0 && !_outgoing.Reader.TryRead(out current))
        {
            response = default;
            return false;
        }

        response = current.Take(index++);
        if (index == current.Count)
        {
            current.Release();
            index = 0;
        }

        return true;
    }

    private void Stage(in PendingResponse response, ref ResponseBatch ready)
    {
        ready.Add(in response);
        if (ready.Count == ReadyBatchLimit)
            Publish(ref ready);
    }

    private void PublishBeforeWait(bool completed, ref ResponseBatch ready)
    {
        if (!completed)
            Publish(ref ready);
    }

    private void Publish(ref ResponseBatch ready)
    {
        if (ready.Count == 0)
            return;

        if (!_outgoing.Writer.TryWrite(ready))
            ready.Release();
        else
            ready = default;
    }

    private async Task PublishOnCompletionAsync(ValueTask<PendingResponse?> pending)
    {
        if (await pending.ConfigureAwait(false) is { } response)
            _ = _outgoing.Writer.TryWrite(new ResponseBatch(in response));
    }

    private async Task WriteAsync()
    {
        var writer = _connection.Transport.Output;
        var stop = _connection.ConnectionClosed;
        var current = default(ResponseBatch);
        var index = 0;
        try
        {
            while (index < current.Count || await _outgoing.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
            {
                stop.ThrowIfCancellationRequested();
                var bytes = 0;
                var count = 0;
                for (; count < Frames.MaxBatch && bytes < Frames.MaxBatchBytes && TryReadResponse(ref current, ref index, out var frame); count++)
                {
                    bytes += WriteResponse(writer, in frame);
                    _ = _slots.Writer.TryWrite(frame.Slot);
                }

                using var boundedFlush = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, _timeProvider, stop);
                var flush = await writer.FlushAsync(boundedFlush.Token).ConfigureAwait(false);
                if (flush is { IsCanceled: false, IsCompleted: false })
                    continue;

                _connection.Abort();
                return;
            }
        }
        catch (Exception error) when (error is IOException or OperationCanceledException)
        {
            _connection.Abort();
        }
        finally
        {
            current.Release();
        }
    }

    private async Task CompleteAsync(Task writer)
    {
        try
        {
#pragma warning disable VSTHRD003 // The connection tasks use ConfigureAwait(false) and unblock on peer disconnect.
            await Task.WhenAll(_jobs).ConfigureAwait(false);
        }
        finally
        {
            _ = _outgoing.Writer.TryComplete();
            try
            {
                await writer.ConfigureAwait(false);
            }
            finally
            {
                while (_outgoing.Reader.TryRead(out var pending))
                    pending.Release();
            }
#pragma warning restore VSTHRD003
        }
    }

    private async ValueTask<PendingResponse?> ExecuteCallAsync(int slot, (int Id, int Size, int Control) frame, byte[] body)
    {
        try
        {
            var response = await _process(frame, body.AsMemory(0, frame.Size)).ConfigureAwait(false);
            return new PendingResponse(slot, frame.Id, response);
        }
        catch (OperationCanceledException) when (_connection.ConnectionClosed.IsCancellationRequested)
        {
            // The transport is closed; the reply has no receiver.
            return null;
        }
#pragma warning disable CA1031 // A failed application call must not terminate its multiplexed peers.
        catch (Exception error)
#pragma warning restore CA1031
        {
            LogCallFailure(_log, frame.Id, error);
            return new PendingResponse(slot, frame.Id, new BinaryResponse(FrameStatus.Unknown));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct PartialFrame(int Slot, (int Id, int Size, int Control) Frame);

    private readonly record struct QueuedCall(int Slot, (int Id, int Size, int Control) Frame, byte[] Body);

    private readonly record struct PendingResponse(int Slot, int Id, BinaryResponse Response);

    private struct ResponseBatch
    {
        private PendingResponse _first;
        private PendingResponse[]? _remaining;

        internal ResponseBatch(in PendingResponse first)
        {
            _first = first;
            Count = 1;
        }

        internal int Count { get; private set; }

        internal PendingResponse Take(int index)
        {
            ref var entry = ref (index == 0 ? ref _first : ref _remaining![index - 1]);
            var response = entry;
            entry = default;
            return response;
        }

        internal void Add(in PendingResponse response)
        {
            if (Count == 0)
                _first = response;
            else
                (_remaining ??= ArrayPool<PendingResponse>.Shared.Rent(MaxPending - 1))[Count - 1] = response;

            Count++;
        }

        internal void Release()
        {
            if (_remaining != null)
                ArrayPool<PendingResponse>.Shared.Return(_remaining, true);

            this = default;
        }
    }

    private sealed record QueuedDispatch(BinaryConnectionPump Pump, QueuedCall[] Batch, int Count);
}
