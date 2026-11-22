using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Dispatches bounded concurrent requests and flushes ready responses from one writer.</summary>
internal sealed class BinaryConnectionPump
{
    private const int MaxPending = 16;
    private const int DispatchBatchLimit = 4;

    private const int InlineRequestBytes = 4 * 1024;
    private const int ReadyBatchLimit = 8;

    private readonly IDuplexPipe _transport;
    private readonly CancellationToken _connectionClosed;
    private readonly Action _abort;
    private readonly IServerLogger _log;
    private readonly TimeProvider _timeProvider;
    private readonly Func<RequestFrame, ReadOnlyMemory<byte>, TimeSpan, Task, ValueTask<BinaryResponse>> _process;
    private readonly LargeCacheRequests? _largeRequests;
    private readonly Task[] _jobs = new Task[MaxPending];
    private readonly Channel<int> _slots = Channel.CreateBounded<int>(MaxPending);
    private readonly Channel<ResponseBatch> _outgoing = Channel.CreateBounded<ResponseBatch>(
        new BoundedChannelOptions(MaxPending) { SingleReader = true });

    private ResponseBatch _ready;
    private Task _previousOrdered = Task.CompletedTask;

    internal BinaryConnectionPump(
        IDuplexPipe transport,
        Action abort,
        Func<RequestFrame, ReadOnlyMemory<byte>, TimeSpan, Task, ValueTask<BinaryResponse>> process,
        IServerLogger log,
        TimeProvider timeProvider,
        CancellationToken connectionClosed,
        LargeCacheRequests? largeRequests = null)
    {
        _transport = transport;
        _connectionClosed = connectionClosed;
        _abort = abort;
        _log = log;
        _timeProvider = timeProvider;
        _process = process;
        _largeRequests = largeRequests;
        for (var i = 0; i < _jobs.Length; i++)
        {
            _jobs[i] = Task.CompletedTask;
            _ = _slots.Writer.TryWrite(i);
        }
    }

    internal async Task RunAsync(CancellationToken readStop)
    {
        var writer = ResponseWriter.RunAsync(this);
        try
        {
            await ReadAsync(readStop).ConfigureAwait(false);
        }
        finally
        {
            await CompleteAsync(writer).ConfigureAwait(false);
        }
    }

    private static void LogCallFailure(IServerLogger logger, int requestId, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.LogCallFailureEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(requestId));
    }

    private async Task ReadAsync(CancellationToken stop)
    {
        var input = _transport.Input;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var read = input.ReadAsync(stop);
                PublishBeforeWait(read.IsCompletedSuccessfully);
                var result = await read.ConfigureAwait(false);
                var partial = ConsumeBuffered(input, result, stop, out var waitingForSlot);
                if (waitingForSlot && !await WaitForSlotAsync(stop).ConfigureAwait(false))
                    return;
                if (partial is { } frame && !await ReadPartialAsync(input, frame, stop).ConfigureAwait(false))
                    return;
            }
        }
        catch (Exception error) when (error is IOException or InvalidDataException)
        {
            _abort();
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            // Allow already accepted calls to complete during graceful shutdown.
        }
        finally
        {
            PublishReady();
        }
    }

    private ValueTask<bool> WaitForSlotAsync(CancellationToken stop)
    {
        var slots = _slots.Reader.WaitToReadAsync(stop);
        PublishBeforeWait(slots.IsCompletedSuccessfully);
        return slots;
    }

    private PartialFrame? ConsumeBuffered(PipeReader input, ReadResult result, CancellationToken stop, out bool waitingForSlot)
    {
        waitingForSlot = false;
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

            while (buffer.Length >= Frames.RequestBytes && !stop.IsCancellationRequested)
            {
                if (!_slots.Reader.TryRead(out var slot))
                {
                    waitingForSlot = true;
                    return null;
                }

                var header = buffer;
                buffer = buffer.Slice(Frames.RequestBytes);
                consumed = examined = buffer.Start;
                var frame = FrameIO.ReadHeader(in header);
                if (frame.Size == Frames.ExtendedSize || buffer.Length < frame.Size)
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

                Dispatch(slot, in frame, body, ref queued, ref queuedCount);
            }

            examined = buffer.End;
            stop.ThrowIfCancellationRequested();

            waitingForSlot = WaitForSlotOnEof(result, buffer.Length);
            return null;
        }
        finally
        {
            input.AdvanceTo(consumed, examined);
            QueueExecution(ref queued, ref queuedCount);
        }
    }

    private bool WaitForSlotOnEof(ReadResult result, long remaining)
    {
        var reachedEof = result.IsCompleted && remaining == result.Buffer.Length;

        // Drain an already accepted call before closing a full connection at EOF.
        return reachedEof && _slots.Reader.TryPeek(out _) ? throw new EndOfStreamException() : reachedEof;
    }

    private void Dispatch(int slot, in RequestFrame frame, byte[] body, ref QueuedCall[]? batch, ref int count)
    {
        var operation = Unsafe.BitCast<int, BinaryOperation>(Frames.ReadOperationId(frame.Control));
        var predecessor = _previousOrdered;
        TaskCompletionSource? completion = null;
        if (operation is not (BinaryOperation.GetDistributed or BinaryOperation.GetValue))
        {
            if (frame.Size <= InlineRequestBytes && predecessor.IsCompletedSuccessfully)
            {
                QueueExecution(ref batch, ref count);
                DispatchInline(new QueuedCall(slot, frame, body, _timeProvider.GetTimestamp(), predecessor, null));
                return;
            }

            completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _previousOrdered = completion.Task;
        }

        batch ??= ArrayPool<QueuedCall>.Shared.Rent(DispatchBatchLimit);
        batch[count++] = new QueuedCall(slot, frame, body, _timeProvider.GetTimestamp(), predecessor, completion);
        if (count == DispatchBatchLimit)
            QueueExecution(ref batch, ref count);
    }

    private void DispatchInline(in QueuedCall call)
    {
        var pending = ExecuteCallAsync(call);
        if (pending.IsCompletedSuccessfully)
        {
            _jobs[call.Slot] = Task.CompletedTask;
#pragma warning disable MA0045 // Completion was checked before taking the synchronous result.
            if (pending.Result is { } response)
#pragma warning restore MA0045
                Stage(in response, ref _ready);
        }
        else
        {
            PublishReady();
            _previousOrdered = _jobs[call.Slot] = PublishOnCompletionAsync(pending);
        }
    }

    private void PublishBeforeWait(bool completed)
    {
        if (!completed)
            PublishReady();
    }

    private void PublishReady() => Publish(ref _ready);

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
                var pending = ExecuteCallAsync(call);
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
        var frame = partial.Frame;
        using (var deadline = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, _timeProvider, stop))
        {
            try
            {
                if (frame.Size == Frames.ExtendedSize)
                    return await ReadExtendedAsync(stream, partial, deadline.Token).ConfigureAwait(false);

                var read = FrameIO.ReadBodyAsync(stream, frame.Size, deadline.Token);
                PublishBeforeWait(read.IsCompletedSuccessfully);
                body = await read.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !stop.IsCancellationRequested)
            {
                // A timed-out body leaves the connection between frame boundaries.
                _abort();
                return false;
            }
        }

        QueuedCall[]? batch = null;
        var count = 0;
        Dispatch(partial.Slot, partial.Frame, body, ref batch, ref count);
        QueueExecution(ref batch, ref count);
        return true;
    }

    private async ValueTask<bool> ReadExtendedAsync(Stream stream, PartialFrame partial, CancellationToken cancellationToken)
    {
        if (_largeRequests is not { } admission)
            throw new InvalidDataException("Extended cache frames were not negotiated.");

        PublishReady();
        var (size, valueBytes) = await FrameIO.ReadExtensionAsync(stream, cancellationToken).ConfigureAwait(false);
        if (admission.Admit(size, valueBytes) is { } refusal)
        {
            var response = new PendingResponse(partial.Slot, partial.Frame.Id, refusal);
            _ = _outgoing.Writer.TryWrite(new ResponseBatch(in response));
            await FrameIO.SkipBodyAsync(stream, size, cancellationToken).ConfigureAwait(false);
            return true;
        }

        try
        {
            var body = GC.AllocateUninitializedArray<byte>(size);
            await stream.ReadExactlyAsync(body, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            QueuedCall[]? batch = null;
            var count = 0;
            Dispatch(partial.Slot, partial.Frame with { Size = size, ValueBytes = valueBytes }, body, ref batch, ref count);
            QueueExecution(ref batch, ref count);
            return true;
        }
        catch
        {
            admission.Release(size);
            throw;
        }
    }

    private void Stage(in PendingResponse response, ref ResponseBatch ready)
    {
        ready = ready.Append(in response);
        if (ready.Count == ReadyBatchLimit)
            Publish(ref ready);
    }

    private void Publish(ref ResponseBatch ready)
    {
        if (ready.Count == 0)
            return;

        if (!_outgoing.Writer.TryWrite(ready))
            ready.Release();
        ready = default;
    }

    private async Task PublishOnCompletionAsync(ValueTask<PendingResponse?> pending)
    {
        if (await pending.ConfigureAwait(false) is { } response)
            _ = _outgoing.Writer.TryWrite(new ResponseBatch(in response));
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

    private async ValueTask<PendingResponse?> ExecuteCallAsync(QueuedCall call)
    {
        try
        {
            if (_connectionClosed.IsCancellationRequested)
                return null;

            var elapsed = _timeProvider.GetElapsedTime(call.Received);
            var response = await _process(call.Frame, call.Body.AsMemory(0, call.Frame.Size), elapsed, call.Predecessor).ConfigureAwait(false);
            return new PendingResponse(call.Slot, call.Frame.Id, response);
        }
        catch (OperationCanceledException) when (_connectionClosed.IsCancellationRequested)
        {
            // The transport is closed; the reply has no receiver.
            return null;
        }
#pragma warning disable CA1031 // A failed application call must not terminate its multiplexed peers.
        catch (Exception error)
#pragma warning restore CA1031
        {
            LogCallFailure(_log, call.Frame.Id, error);
            return new PendingResponse(call.Slot, call.Frame.Id, new BinaryResponse(FrameStatus.Unknown));
        }
        finally
        {
            if (call.Frame.ValueBytes != null)
                _largeRequests!.Release(call.Frame.Size);
            else
                ArrayPool<byte>.Shared.Return(call.Body);
            if (call.Completion is { } completion)
            {
                // A rejected or expired request still links its successor to older mutations.
                await call.Predecessor.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                completion.SetResult();
            }
        }
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct RequestFrame(int Id, int Size, int Control, int? ValueBytes = null);

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct PartialFrame(int Slot, RequestFrame Frame);

    private readonly record struct QueuedCall(
        int Slot,
        RequestFrame Frame,
        byte[] Body,
        long Received,
        Task Predecessor,
        TaskCompletionSource? Completion);

    private readonly record struct PendingResponse(int Slot, int Id, BinaryResponse Response);

    [Mutable]
    private readonly record struct ResponseBatch(PendingResponse First, PendingResponse[]? Remaining, int Count)
    {
        internal ResponseBatch(in PendingResponse first)
            : this(first, null, 1)
        {
        }

        internal static bool TryRead(ChannelReader<ResponseBatch> reader, ref ResponseBatch current, ref int index, out PendingResponse response)
        {
            if (current.Count == 0 && !reader.TryRead(out current))
            {
                response = default;
                return false;
            }

            response = Take(ref current, index++);
            if (index == current.Count)
            {
                current.Release();
                current = default;
                index = 0;
            }

            return true;
        }

        internal ResponseBatch Append(in PendingResponse response)
        {
            if (Count == 0)
                return new ResponseBatch(in response);

            var remaining = Remaining ?? ArrayPool<PendingResponse>.Shared.Rent(MaxPending - 1);
            remaining[Count - 1] = response;
            return new ResponseBatch(First, remaining, Count + 1);
        }

        internal void Release()
        {
            if (Remaining != null)
                ArrayPool<PendingResponse>.Shared.Return(Remaining, true);
        }

        private static PendingResponse Take(ref ResponseBatch batch, int index)
        {
            if (index == 0)
            {
                var first = batch.First;
                batch = batch with { First = default };
                return first;
            }

            ref var entry = ref batch.Remaining![index - 1];
            var response = entry;
            entry = default;
            return response;
        }
    }

    private static class FrameIO
    {
        internal static async ValueTask<(int Size, int ValueBytes)> ReadExtensionAsync(Stream input, CancellationToken cancellationToken)
        {
            var bytes = ArrayPool<byte>.Shared.Rent(Frames.ExtendedRequestBytes);
            try
            {
                await input.ReadExactlyAsync(bytes.AsMemory(0, Frames.ExtendedRequestBytes), cancellationToken).ConfigureAwait(false);
                return (Frames.ReadExtendedSize(bytes), BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(sizeof(int))));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }

        internal static async ValueTask SkipBodyAsync(Stream input, int size, CancellationToken cancellationToken)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(Frames.MaxBatchBytes);
            try
            {
                while (size > 0)
                {
                    var length = Math.Min(size, Frames.MaxBatchBytes);
                    await input.ReadExactlyAsync(buffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                    size -= length;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        internal static async ValueTask<byte[]> ReadBodyAsync(Stream input, int size, CancellationToken cancellationToken)
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

        internal static RequestFrame ReadHeader(in ReadOnlySequence<byte> buffer)
        {
            var first = buffer.FirstSpan;
            if (first.Length >= Frames.RequestBytes)
                return ReadHeader(first);

            Span<byte> header = stackalloc byte[Frames.RequestBytes];
            buffer.Slice(0, Frames.RequestBytes).CopyTo(header);
            return ReadHeader(header);
        }

        private static RequestFrame ReadHeader(ReadOnlySpan<byte> header)
        {
            var (id, size, control) = Frames.ReadRequest(header);
            return new RequestFrame(id, size, control);
        }
    }

    private static class ResponseWriter
    {
        internal static async Task RunAsync(BinaryConnectionPump connection)
        {
            var writer = connection._transport.Output;
            var stop = connection._connectionClosed;
            var current = default(ResponseBatch);
            var index = 0;
            try
            {
                while (index < current.Count || await connection._outgoing.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
                {
                    stop.ThrowIfCancellationRequested();
                    var bytes = 0;
                    var count = 0;
                    for (; count < Frames.MaxBatch && bytes < Frames.MaxBatchBytes && ResponseBatch.TryRead(connection._outgoing.Reader, ref current, ref index, out var frame); count++)
                    {
                        var size = frame.Response.Payload?.CalculateSize() ?? 0;
                        if (connection._largeRequests != null && size is > Frames.MaxResponseBytes and <= Frames.MaxExtendedBodyBytes)
                        {
                            using var transfer = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, connection._timeProvider, stop);
                            await WriteExtendedResponseAsync(writer, frame, size, transfer.Token).ConfigureAwait(false);
                            bytes += size;
                        }
                        else
                        {
                            bytes += WriteResponse(writer, in frame, size);
                        }

                        _ = connection._slots.Writer.TryWrite(frame.Slot);
                    }

                    using var boundedFlush = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, connection._timeProvider, stop);
                    var flush = await writer.FlushAsync(boundedFlush.Token).ConfigureAwait(false);
                    if (flush is { IsCanceled: false, IsCompleted: false })
                        continue;

                    connection._abort();
                    return;
                }
            }
            catch (Exception error) when (error is IOException or OperationCanceledException)
            {
                connection._abort();
            }
            finally
            {
                current.Release();
            }
        }

        private static async ValueTask WriteExtendedResponseAsync(PipeWriter writer, PendingResponse frame, int size, CancellationToken cancellationToken)
        {
            using var segments = new WireSegments();
            const int headerBytes = Frames.ReplyBytes + Frames.ExtendedReplyBytes;
            var header = segments.GetMemory(headerBytes);
            Frames.WriteReply(header.Span, frame.Id, Frames.ExtendedSize, Unsafe.BitCast<FrameStatus, int>(frame.Response.Status));
            Frames.WriteExtendedSize(header.Span[Frames.ReplyBytes..], size);
            segments.Advance(headerBytes);
            frame.Response.Payload!.WriteTo(segments);
            foreach (var chunk in segments.Chunks())
            {
                var flush = await writer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
                if (flush.IsCanceled || flush.IsCompleted)
                    throw new IOException("Extended cache reply was interrupted.");
            }
        }

        private static int WriteResponse(PipeWriter writer, in PendingResponse frame, int size)
        {
            var payload = frame.Response.Payload;
            var status = frame.Response.Status;
            if (size > Frames.MaxResponseBytes)
            {
                size = 0;
                status = FrameStatus.ResourceExhausted;
            }

            var length = Frames.ReplyBytes + size;
            var target = writer.GetSpan(length);
            if (size == 0 || payload == null)
                Frames.WriteReply(target, frame.Id, 0, Unsafe.BitCast<FrameStatus, int>(status));
            else
                Frames.WriteReplyMessage(target, frame.Id, size, Unsafe.BitCast<FrameStatus, int>(status), payload);

            writer.Advance(length);
            return length;
        }
    }

    private sealed record QueuedDispatch(BinaryConnectionPump Pump, QueuedCall[] Batch, int Count);

    private static class Events
    {
        internal static readonly LogEvent LogCallFailureEvent = new(3032, nameof(LogCallFailure), "Binary cache request {RequestId} failed");
    }
}
