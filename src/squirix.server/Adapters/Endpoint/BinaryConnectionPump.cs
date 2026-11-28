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
    private MultiplexedQueues? _queues;

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
    }

    /// <summary>Gets the queues initialized by the reader before it starts concurrent dispatch.</summary>
    private MultiplexedQueues Queues => _queues!;

    internal async Task RunAsync(CancellationToken readStop)
    {
        using var flush = new ConnectionFlush(_timeProvider, _connectionClosed);
        await RunPumpsAsync(flush, readStop).ConfigureAwait(false);
    }

    private async Task RunPumpsAsync(ConnectionFlush flush, CancellationToken readStop)
    {
        Task? writer = null;
        try
        {
            if (!await BufferedDispatch.RunAsync(this, flush, readStop).ConfigureAwait(false))
                return;
            _queues ??= new MultiplexedQueues(false);
            writer = ResponseWriter.RunAsync(this, flush);
            await ReadAsync(readStop).ConfigureAwait(false);
        }
        finally
        {
            if (_queues != null)
            {
                writer ??= ResponseWriter.RunAsync(this, flush);
                await CompleteAsync(writer).ConfigureAwait(false);
            }
        }
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
        var slots = Queues.Slots.Reader.WaitToReadAsync(stop);
        PublishBeforeWait(slots.IsCompletedSuccessfully);
        return slots;
    }

    private PartialFrame? ConsumeBuffered(PipeReader input, ReadResult result, CancellationToken stop, out bool waitingForSlot)
    {
        waitingForSlot = false;
        var buffer = result.Buffer;
        var (page, pageOffset) = BinaryReadPage.Find(in buffer);
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
                if (!Queues.Slots.Reader.TryRead(out var slot))
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

                var body = FrameBody.Read(in buffer, frame.Size, page, pageOffset + Frames.RequestBytes);
                pageOffset += Frames.RequestBytes + frame.Size;
                buffer = buffer.Slice(frame.Size);
                consumed = examined = buffer.Start;
                if (stop.IsCancellationRequested)
                {
                    body.DiscardUndispatched();
                    break;
                }

                Dispatch(slot, in frame, in body, ref queued, ref queuedCount);
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
        return reachedEof && Queues.Slots.Reader.TryPeek(out _) ? throw new EndOfStreamException() : reachedEof;
    }

    private void Dispatch(int slot, in RequestFrame frame, in FrameBody body, ref QueuedCall[]? batch, ref int count)
    {
        var predecessor = _previousOrdered;
        TaskCompletionSource? completion = null;
        if (frame.RequiresMutationOrder)
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

        body.Page?.Retain();
        batch ??= ArrayPool<QueuedCall>.Shared.Rent(DispatchBatchLimit);
        batch[count++] = new QueuedCall(slot, frame, body, _timeProvider.GetTimestamp(), predecessor, completion);
        if (count == DispatchBatchLimit)
            QueueExecution(ref batch, ref count);
    }

    private void DispatchInline(in QueuedCall call)
    {
        var pending = ExecuteCallAsync(call, call.Body.Page != null);
        if (pending.IsCompletedSuccessfully)
        {
            Queues.Jobs[call.Slot] = Task.CompletedTask;
#pragma warning disable MA0045 // Completion was checked before taking the synchronous result.
            if (pending.Result is { } response)
#pragma warning restore MA0045
                Stage(in response, ref _ready);
        }
        else
        {
            PublishReady();
            _previousOrdered = Queues.Jobs[call.Slot] = PublishOnCompletionAsync(pending);
        }
    }

    private void HandOffPending(int slot, in RequestFrame frame, ValueTask<PendingResponse?> pending)
    {
        var job = Queues.Jobs[slot] = PublishOnCompletionAsync(pending);
        if (frame.RequiresMutationOrder)
            _previousOrdered = job;
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
            Queues.Jobs[slots[i]] = job;
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
        Dispatch(partial.Slot, partial.Frame, new FrameBody(body), ref batch, ref count);
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
            _ = Queues.Outgoing.Writer.TryWrite(new ResponseBatch(in response));
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
            Dispatch(partial.Slot, partial.Frame with { Size = size, ValueBytes = valueBytes }, new FrameBody(body), ref batch, ref count);
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

        if (!Queues.Outgoing.Writer.TryWrite(ready))
            ready.Release();
        ready = default;
    }

    private async Task PublishOnCompletionAsync(ValueTask<PendingResponse?> pending)
    {
        if (await pending.ConfigureAwait(false) is { } response)
            _ = Queues.Outgoing.Writer.TryWrite(new ResponseBatch(in response));
    }

    private async Task CompleteAsync(Task writer)
    {
        try
        {
#pragma warning disable VSTHRD003 // The connection tasks use ConfigureAwait(false) and unblock on peer disconnect.
            await Task.WhenAll(Queues.Jobs).ConfigureAwait(false);
        }
        finally
        {
            _ = Queues.Outgoing.Writer.TryComplete();
            try
            {
                await writer.ConfigureAwait(false);
            }
            finally
            {
                while (Queues.Outgoing.Reader.TryRead(out var pending))
                    pending.Release();
            }
#pragma warning restore VSTHRD003
        }
    }

    private async ValueTask<PendingResponse?> ExecuteCallAsync(QueuedCall call, bool borrowUntilReturn = false)
    {
        try
        {
            if (_connectionClosed.IsCancellationRequested)
                return null;

            var elapsed = _timeProvider.GetElapsedTime(call.Received);
            var pending = _process(call.Frame, call.Body.Bytes.AsMemory(call.Body.Offset, call.Frame.Size), elapsed, call.Predecessor);

            // The reader still owns this page until inline dispatch returns to ConsumeBuffered.
            if (borrowUntilReturn && !pending.IsCompleted)
            {
                call.Body.Page!.Retain();
                borrowUntilReturn = false;
            }

            var response = await pending.ConfigureAwait(false);
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
            if (_log.IsEnabled(ServerLogLevel.Error))
                Events.CallFailure.Write(_log, ServerLogLevel.Error, error, ValueTuple.Create(call.Frame.Id));
            return new PendingResponse(call.Slot, call.Frame.Id, new BinaryResponse(FrameStatus.Unknown));
        }
        finally
        {
            if (call.Frame.ValueBytes != null)
                _largeRequests!.Release(call.Frame.Size);
            else if (!borrowUntilReturn)
                call.Body.Release();
            if (call.Completion is { } completion)
            {
                // A rejected or expired request still links its successor to older mutations.
                await call.Predecessor.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                completion.SetResult();
            }
        }
    }

    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct RequestFrame(int Id, int Size, int Control, int? ValueBytes = null)
    {
        internal bool RequiresMutationOrder => Unsafe.BitCast<int, BinaryOperation>(Frames.ReadOperationId(Control))
            is not (BinaryOperation.GetDistributed or BinaryOperation.GetValue);
    }

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct PartialFrame(int Slot, RequestFrame Frame);

    private readonly record struct QueuedCall(
        int Slot,
        RequestFrame Frame,
        FrameBody Body,
        long Received,
        Task Predecessor,
        TaskCompletionSource? Completion);

    private readonly record struct FrameBody(byte[] Bytes, int Offset = 0, BinaryReadPage? Page = null)
    {
        internal static FrameBody Read(in ReadOnlySequence<byte> input, int size, BinaryReadPage? contiguousPage, int bodyOffset)
        {
            if (contiguousPage != null)
                return new FrameBody(contiguousPage.Bytes, bodyOffset, contiguousPage);

            var first = input.First;
            if (size <= first.Length && BinaryReadPage.Find(first) is ({ } page, var offset))
                return new FrameBody(page.Bytes, offset, page);

            var bytes = ArrayPool<byte>.Shared.Rent(size);
            input.Slice(0, size).CopyTo(bytes);
            return new FrameBody(bytes);
        }

        internal void DiscardUndispatched()
        {
            // A page has no call reference until dispatch; only a copied body needs returning here.
            if (Page == null)
                ArrayPool<byte>.Shared.Return(Bytes);
        }

        internal void Release()
        {
            if (Page == null)
                ArrayPool<byte>.Shared.Return(Bytes);
            else
                Page.Release();
        }
    }

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

        internal static RequestFrame ReadHeader(ReadOnlySpan<byte> header)
        {
            var (id, size, control) = Frames.ReadRequest(header);
            return new RequestFrame(id, size, control);
        }
    }

    /// <summary>Dispatches contiguous pages directly until an operation needs the multiplexed reader and writer.</summary>
    private static class BufferedDispatch
    {
        private const int MaxFrames = 128;

        internal static async Task<bool> RunAsync(BinaryConnectionPump pump, ConnectionFlush output, CancellationToken stop)
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var read = await pump._transport.Input.ReadAsync(stop).ConfigureAwait(false);
                    var (transfer, eof, bytes) = await ConsumeAsync(pump, read, stop).ConfigureAwait(false);
                    if (bytes != 0 && !await FlushAsync(pump, output).ConfigureAwait(false))
                        return false;
                    if (transfer || eof)
                        return transfer;
                }
            }
            catch (Exception error) when (error is IOException or InvalidDataException or OperationCanceledException)
            {
                if (!stop.IsCancellationRequested)
                    pump._abort();
            }

            return false;
        }

        private static async ValueTask<(bool Transfer, bool Eof, int Bytes)> ConsumeAsync(BinaryConnectionPump pump, ReadResult read, CancellationToken stop)
        {
            var buffer = read.Buffer;
            var consumed = 0;
            var bytes = 0;
            try
            {
                if (ShouldStop(read))
                    return (false, read.IsCanceled || read.IsCompleted, 0);
                if (BinaryReadPage.Find(in buffer) is not ({ } page, var offset))
                    return (true, false, 0);

                var input = buffer.First;
                var output = Memory<byte>.Empty;
                for (var count = 0; count < MaxFrames && consumed < input.Length && !stop.IsCancellationRequested; count++)
                {
                    if (!TryRead(input.Span[consumed..], out var frame))
                        return (true, false, bytes);

                    var body = new FrameBody(page.Bytes, offset + consumed + Frames.RequestBytes, page);
                    consumed += Frames.RequestBytes + frame.Size;
                    var call = new QueuedCall(0, frame, body, pump._timeProvider.GetTimestamp(), Task.CompletedTask, null);
                    var pending = pump.ExecuteCallAsync(call, true);
                    if (!pending.IsCompletedSuccessfully)
                    {
                        pump._queues = new MultiplexedQueues(true);
                        pump.HandOffPending(0, in frame, pending);
                        return (true, false, bytes);
                    }

                    if (await pending.ConfigureAwait(false) is not { } reply)
                        break;
                    var written = EncodeOrTransfer(pump, in reply, ref output, bytes);
                    if (written < 0)
                        return (true, false, bytes);
                    bytes += written;
                }

                return (false, read.IsCompleted && consumed == input.Length, bytes);
            }
            finally
            {
                if (bytes != 0)
                    pump._transport.Output.Advance(bytes);
                var position = buffer.GetPosition(consumed);
                pump._transport.Input.AdvanceTo(position, position);
            }
        }

        private static bool ShouldStop(ReadResult read) => read.IsCanceled || read.Buffer.IsEmpty;

        private static int EncodeOrTransfer(BinaryConnectionPump pump, in PendingResponse reply, ref Memory<byte> output, int used)
        {
            if (output.IsEmpty)
                output = pump._transport.Output.GetMemory(Frames.MaxBatchBytes)[..Frames.MaxBatchBytes];
            var target = output.Span[used..];
            var payload = reply.Response.Payload;
            var size = payload?.CalculateSize() ?? 0;
            var length = Frames.ReplyBytes + size;
            if (length > target.Length)
            {
                pump._queues = new MultiplexedQueues(true);
                _ = pump._queues.Outgoing.Writer.TryWrite(new ResponseBatch(in reply));
                return -1;
            }

            var status = Unsafe.BitCast<FrameStatus, int>(reply.Response.Status);
            if (size == 0 || payload == null)
                Frames.WriteReply(target, reply.Id, 0, status);
            else
                Frames.WriteReplyMessage(target, reply.Id, size, status, payload);
            return length;
        }

        private static async ValueTask<bool> FlushAsync(BinaryConnectionPump pump, ConnectionFlush output)
        {
            var flush = await output.WriteAsync(pump._transport.Output).ConfigureAwait(false);
            if (!flush.IsCanceled && !flush.IsCompleted)
                return true;

            pump._abort();
            return false;
        }

        private static bool TryRead(ReadOnlySpan<byte> bytes, out RequestFrame frame)
        {
            frame = default;
            if (bytes.Length < Frames.RequestBytes)
                return false;
            frame = FrameIO.ReadHeader(bytes);
            return frame.Size <= InlineRequestBytes && bytes.Length >= Frames.RequestBytes + frame.Size;
        }
    }

    private static class ResponseWriter
    {
        internal static async Task RunAsync(BinaryConnectionPump connection, ConnectionFlush output)
        {
            var writer = connection._transport.Output;
            var stop = connection._connectionClosed;
            var current = default(ResponseBatch);
            var index = 0;
            try
            {
                while (index < current.Count || await connection.Queues.Outgoing.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
                {
                    stop.ThrowIfCancellationRequested();
                    var bytes = 0;
                    var count = 0;
                    for (; count < Frames.MaxBatch && bytes < Frames.MaxBatchBytes && ResponseBatch.TryRead(connection.Queues.Outgoing.Reader, ref current, ref index, out var frame); count++)
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

                        _ = connection.Queues.Slots.Writer.TryWrite(frame.Slot);
                    }

                    var flush = await output.WriteAsync(writer).ConfigureAwait(false);
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

        internal static int WriteResponse(PipeWriter writer, in PendingResponse frame, int size)
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
    }

    private sealed record QueuedDispatch(BinaryConnectionPump Pump, QueuedCall[] Batch, int Count);

    private static class Events
    {
        internal static readonly LogEvent CallFailure = new(3032, "LogCallFailure", "Binary cache request {RequestId} failed");
    }

    private sealed class MultiplexedQueues
    {
        internal MultiplexedQueues(bool firstSlotInUse)
        {
            Array.Fill(Jobs, Task.CompletedTask);
            for (var slot = firstSlotInUse ? 1 : 0; slot < MaxPending; slot++)
                _ = Slots.Writer.TryWrite(slot);
        }

        internal Task[] Jobs { get; } = new Task[MaxPending];

        internal Channel<int> Slots { get; } = Channel.CreateBounded<int>(MaxPending);

        internal Channel<ResponseBatch> Outgoing { get; } = Channel.CreateBounded<ResponseBatch>(
            new BoundedChannelOptions(MaxPending) { SingleReader = true });
    }
}
