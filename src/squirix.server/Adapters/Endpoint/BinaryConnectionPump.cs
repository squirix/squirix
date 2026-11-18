using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Dispatches bounded concurrent requests and flushes ready responses from one writer.</summary>
internal sealed partial class BinaryConnectionPump
{
    private const int MaxPending = 16;
    private const int ReadyBatchLimit = 8;

    private readonly ConnectionContext _connection;
    private readonly ILogger _log;
    private readonly TimeProvider _timeProvider;
    private readonly Func<(int Id, int Size, int Control), ReadOnlyMemory<byte>, ValueTask<BinaryResponse>> _process;
    private readonly Task[] _jobs = new Task[MaxPending];
    private readonly Channel<int> _slots = Channel.CreateBounded<int>(MaxPending);
    private readonly Channel<ResponseBatch> _outgoing = Channel.CreateBounded<ResponseBatch>(
        new BoundedChannelOptions(MaxPending) { SingleReader = true });

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
            status = StatusCode.ResourceExhausted;
        }

        var length = Frames.HeaderBytes + size;
        var target = writer.GetSpan(length);
        if (size == 0 || payload == null)
            Frames.Write(target, frame.Id, 0, Unsafe.BitCast<StatusCode, int>(status));
        else
            Frames.WriteMessage(target, frame.Id, size, Unsafe.BitCast<StatusCode, int>(status), payload);

        writer.Advance(length);
        return length;
    }

    private async Task ReadAsync(CancellationToken stop)
    {
        var input = _connection.Transport.Input.AsStream(true);
        await using var inputLifetime = input.ConfigureAwait(false);
        var header = ArrayPool<byte>.Shared.Rent(Frames.HeaderBytes);
        var ready = default(ResponseBatch);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var nextSlot = _slots.Reader.ReadAsync(stop);
                PublishBeforeWait(nextSlot.IsCompletedSuccessfully, ref ready);
                var slot = await nextSlot.ConfigureAwait(false);
                var headerRead = input.ReadExactlyAsync(header.AsMemory(0, Frames.HeaderBytes), stop);
                PublishBeforeWait(headerRead.IsCompletedSuccessfully, ref ready);
                await headerRead.ConfigureAwait(false);
                var frame = Frames.Read(header, Frames.MaxRequestBytes);
                byte[] body;
                using (var deadline = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, _timeProvider, stop))
                {
                    try
                    {
                        var bodyRead = ReadBodyAsync(input, frame.Size, deadline.Token);
                        PublishBeforeWait(bodyRead.IsCompletedSuccessfully, ref ready);
                        body = await bodyRead.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (deadline.IsCancellationRequested && !stop.IsCancellationRequested)
                    {
                        // A timed-out body leaves the connection between frame boundaries.
                        _connection.Abort();
                        return;
                    }
                }

                var pending = ExecuteCallAsync(slot, frame, body);
                if (!pending.IsCompletedSuccessfully)
                {
                    _jobs[slot] = PublishOnCompletionAsync(pending);
                    continue;
                }

                _jobs[slot] = Task.CompletedTask;
                if (await pending.ConfigureAwait(false) is { } response)
                    Stage(in response, ref ready);
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
            Publish(ref ready);
            ArrayPool<byte>.Shared.Return(header);
        }
    }

    private bool TryReadResponse(ref ResponseBatch current, ref int index, out PendingResponse response)
    {
        if (current.Count == 0 && !_outgoing.Reader.TryRead(out current))
        {
            response = default;
            return false;
        }

        response = current[index++];
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
            return new PendingResponse(slot, frame.Id, new BinaryResponse(StatusCode.Unknown));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

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

        internal readonly PendingResponse this[int index] => index == 0 ? _first : _remaining![index - 1];

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
}
