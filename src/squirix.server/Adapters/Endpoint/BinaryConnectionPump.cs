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

    private readonly ConnectionContext _connection;
    private readonly ILogger _log;
    private readonly Func<(int Id, int Size, int Control), ReadOnlyMemory<byte>, ValueTask<BinaryResponse>> _process;
    private readonly Task[] _jobs = new Task[MaxPending];
    private readonly Channel<int> _slots = Channel.CreateBounded<int>(MaxPending);
    private readonly Channel<PendingResponse> _outgoing = Channel.CreateBounded<PendingResponse>(
        new BoundedChannelOptions(MaxPending) { SingleReader = true });

    internal BinaryConnectionPump(ConnectionContext connection, Func<(int Id, int Size, int Control), ReadOnlyMemory<byte>, ValueTask<BinaryResponse>> process, ILogger log)
    {
        _connection = connection;
        _log = log;
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
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var slot = await _slots.Reader.ReadAsync(stop).ConfigureAwait(false);
                await input.ReadExactlyAsync(header.AsMemory(0, Frames.HeaderBytes), stop).ConfigureAwait(false);
                var frame = Frames.Read(header, Frames.MaxRequestBytes);
                var body = await ReadBodyAsync(input, frame.Size, stop).ConfigureAwait(false);
                _jobs[slot] = RunCallAsync(slot, frame, body);
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
            ArrayPool<byte>.Shared.Return(header);
        }
    }

    private async Task WriteAsync()
    {
        var writer = _connection.Transport.Output;
        var stop = _connection.ConnectionClosed;
        try
        {
            while (await _outgoing.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
            {
                var bytes = 0;
                for (var count = 0; count < Frames.MaxBatch && bytes < Frames.MaxBatchBytes && _outgoing.Reader.TryRead(out var frame); count++)
                {
                    bytes += WriteResponse(writer, in frame);
                    _ = _slots.Writer.TryWrite(frame.Slot);
                }

                using var boundedFlush = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, TimeProvider.System, stop);
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
            await writer.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
    }

    private async Task RunCallAsync(int slot, (int Id, int Size, int Control) frame, byte[] body)
    {
        try
        {
            var response = await _process(frame, body.AsMemory(0, frame.Size)).ConfigureAwait(false);
            _ = _outgoing.Writer.TryWrite(new PendingResponse(slot, frame.Id, response));
        }
        catch (OperationCanceledException) when (_connection.ConnectionClosed.IsCancellationRequested)
        {
            // The transport is closed; the reply has no receiver.
        }
#pragma warning disable CA1031 // A failed application call must not terminate its multiplexed peers.
        catch (Exception error)
#pragma warning restore CA1031
        {
            LogCallFailure(_log, frame.Id, error);
            _ = _outgoing.Writer.TryWrite(new PendingResponse(slot, frame.Id, new BinaryResponse(StatusCode.Unknown)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
        }
    }

    private readonly record struct PendingResponse(int Slot, int Id, BinaryResponse Response);
}
