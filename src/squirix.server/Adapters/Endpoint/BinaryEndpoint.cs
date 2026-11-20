using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal static class BinaryEndpoint
{
    private const string ApiPath = "/squirix.cache.v2.SquirixCacheService/";

    private static readonly string[] OperationPaths =
    [
        ApiPath + nameof(BinaryOperation.Unspecified),
        ApiPath + nameof(BinaryOperation.SetEntry),
        ApiPath + nameof(BinaryOperation.GetEntry),
        ApiPath + nameof(BinaryOperation.GetValue),
        ApiPath + nameof(BinaryOperation.Remove),
        ApiPath + nameof(BinaryOperation.TryAddEntry),
        ApiPath + nameof(BinaryOperation.Touch),
        ApiPath + nameof(BinaryOperation.RemoveExpiration),
        ApiPath + nameof(BinaryOperation.Update),
        ApiPath + nameof(BinaryOperation.GetExpiration),
        ApiPath + nameof(BinaryOperation.GetOrAdd),
        ApiPath + nameof(BinaryOperation.SetDistributed),
        ApiPath + nameof(BinaryOperation.GetDistributed),
        ApiPath + nameof(BinaryOperation.RefreshDistributed),
    ];

    internal static Func<BinaryConnection, Task> Create(IServiceProvider services, bool mutationReplay)
    {
        var validator = services.GetService<NativeConnectionAuthenticationValidator>();
        var auth = new ConnectionAuthenticationMiddleware(connection => ServeAsync(connection, services, validator), validator, TimeProvider.System);
        var accepted = mutationReplay ? Frames.AcceptedWithMutationReplay : Frames.Accepted;
        return connection => AcceptAsync(connection, auth, services, validator != null, accepted);
    }

    private static async Task AcceptAsync(BinaryConnection connection, ConnectionAuthenticationMiddleware authenticate, IServiceProvider services, bool requiresCredentials, byte accepted)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, connection.CloseRequested);
        using var timeout = new TimedCancellation(TimeSpan.FromSeconds(5), TimeProvider.System, stop.Token);
        var (hasPreface, mode) = await ReadPrefaceAsync(connection.Transport.Input, timeout.Token).ConfigureAwait(false);
        if (!hasPreface)
            return;
        var allowed = mode == 1 ? requiresCredentials : mode == 0 && !requiresCredentials;
        var writer = connection.Transport.Output;
        writer.GetSpan(1)[0] = allowed ? accepted : Frames.Rejected;
        writer.Advance(1);
        _ = await writer.FlushAsync(timeout.Token).ConfigureAwait(false);
        if (!allowed)
            return;
        if (mode == 1)
            await authenticate.OnConnectionAsync(connection).ConfigureAwait(false);
        else
            await ServeAsync(connection, services, null).ConfigureAwait(false);
    }

    private static async ValueTask<(bool HasPreface, byte Mode)> ReadPrefaceAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        var result = await reader.ReadAtLeastAsync(Frames.PrefaceBytes, cancellationToken).ConfigureAwait(false);
        var buffer = result.Buffer;
        var consumed = buffer.Start;
        try
        {
            if (buffer.Length < Frames.PrefaceBytes)
                return default;

            var mode = ParsePreface(buffer);
            if (mode == null)
                return default;

            consumed = buffer.GetPosition(Frames.PrefaceBytes);
            return (true, mode.Value);
        }
        finally
        {
            reader.AdvanceTo(consumed, consumed);
        }
    }

    private static byte? ParsePreface(ReadOnlySequence<byte> buffer)
    {
        Span<byte> preface = stackalloc byte[Frames.PrefaceBytes];
        buffer.Slice(0, preface.Length).CopyTo(preface);
        return BinaryPrimitives.ReadUInt32BigEndian(preface) == Frames.Preface ? preface[4] : null;
    }

    private static async Task ServeAsync(BinaryConnection connection, IServiceProvider services, NativeConnectionAuthenticationValidator? validator)
    {
        if (validator != null && connection.Identity == null)
        {
            connection.Abort();
            return;
        }

        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, connection.CloseRequested);
        var accessor = services.GetRequiredService<BinaryRequestAccessor>();
        var operations = services.GetRequiredService<BinaryDispatch>();
        var state = new CallState(connection, services, validator, accessor, operations);
        var log = services.GetRequiredService<ILogger<BinaryConnectionPump>>();
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (frame, bytes, elapsed, predecessor) => ProcessAsync(state, frame, bytes, elapsed, predecessor),
            log,
            TimeProvider.System,
            connection.ConnectionClosed);
        await pump.RunAsync(lifetime.Token).ConfigureAwait(false);
    }

    private static async ValueTask<BinaryResponse> ProcessAsync(CallState state, (int Id, int Size, int Control) frame, ReadOnlyMemory<byte> body, TimeSpan elapsed, Task predecessor)
    {
        var budget = frame.Control & 0xffff;
        var timeout = TimeSpan.FromMilliseconds(Math.Min(budget, RpcAttemptTimeout.OperationDeadline.TotalMilliseconds)) - elapsed;
        if (timeout <= TimeSpan.Zero)
            return new BinaryResponse(FrameStatus.DeadlineExceeded);
        var operationId = Frames.ReadOperationId(frame.Control);
        var operation = Unsafe.BitCast<int, BinaryOperation>(operationId);
        var deadlineUtc = (TimeProvider.System.GetUtcNow() + timeout).UtcDateTime;
        using var deadline = new TimedCancellation(timeout, TimeProvider.System, state.Connection.ConnectionClosed);
        var scope = state.Services.CreateAsyncScope();
        await using var scopeLifetime = scope.ConfigureAwait(false);
        var call = new BinaryRequestContext
        {
            Connection = state.Connection,
            RequestServices = scope.ServiceProvider,
            RequestAborted = deadline.Token,
            Path = operationId < OperationPaths.Length ? OperationPaths[operationId] : OperationPaths[0],
        };
        state.Accessor.Current = call;
        try
        {
            var failure = state.Validator == null ? FrameStatus.Ok : await state.Validator.AuthorizeAsync(state.Connection.Identity!, call, deadline.Token).ConfigureAwait(false);
            deadline.Token.ThrowIfCancellationRequested();
            if (failure != FrameStatus.Ok)
                return new BinaryResponse(failure);

            if (!TryReadTrace(call, frame.Control, ref body))
                return new BinaryResponse(FrameStatus.InvalidArgument);

            var callContext = new BinaryDispatch.Request(call, deadlineUtc, predecessor);
            return await state.Operations.ExecuteAsync(operation, body, in callContext).ConfigureAwait(false);
        }
        catch (ServerCallException error)
        {
            return new BinaryResponse(error.StatusCode, error.Error);
        }
        catch (InvalidProtocolBufferException)
        {
            return new BinaryResponse(FrameStatus.InvalidArgument);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !state.Connection.ConnectionClosed.IsCancellationRequested)
        {
            return new BinaryResponse(FrameStatus.DeadlineExceeded);
        }
        finally
        {
            state.Accessor.Current = null;
        }
    }

    private static bool TryReadTrace(BinaryRequestContext call, int control, ref ReadOnlyMemory<byte> body)
    {
        if ((control & Frames.TracedRequest) == 0)
            return true;

        var traced = BinaryTracedRequest.Parser.ParseFrom(body.Span);
        if (traced.TraceParent.Length > Frames.MaxTraceParentLength || traced.TraceState.Length > Frames.MaxTraceStateLength)
            return false;

        call.TraceParent = traced.TraceParent;
        call.TraceState = traced.TraceState.Length == 0 ? null : traced.TraceState;
        body = traced.Payload.Memory;
        return true;
    }

    private sealed record CallState(
        BinaryConnection Connection,
        IServiceProvider Services,
        NativeConnectionAuthenticationValidator? Validator,
        BinaryRequestAccessor Accessor,
        BinaryDispatch Operations);
}
