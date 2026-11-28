using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
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
        var validator = services.Find<NativeConnectionAuthenticationValidator>();
        var auth = new ConnectionAuthenticationMiddleware(connection => ServeAsync(connection, services, validator), validator, TimeProvider.System);
        var accepted = mutationReplay ? Frames.AcceptedWithMutationReplay : Frames.Accepted;
        return connection => AcceptAsync(connection, auth, services, validator != null, accepted);
    }

    internal static string GetOperationPath(BinaryOperation operation) => OperationPaths[Unsafe.BitCast<BinaryOperation, int>(operation)];

    private static async Task AcceptAsync(BinaryConnection connection, ConnectionAuthenticationMiddleware authenticate, IServiceProvider services, bool requiresCredentials, byte accepted)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, connection.CloseRequested);
        using var timeout = new TimedCancellation(TimeSpan.FromSeconds(5), TimeProvider.System, stop.Token);
        var (hasPreface, mode) = await ReadPrefaceAsync(connection.Transport.Input, timeout.Token).ConfigureAwait(false);
        if (!hasPreface)
            return;
        var credentials = mode & ~Frames.ExtendedMode;
        var allowed = credentials == 1 ? requiresCredentials : credentials == 0 && !requiresCredentials;
        var writer = connection.Transport.Output;
        writer.GetSpan(1)[0] = allowed ? accepted : Frames.Rejected;
        writer.Advance(1);
        _ = await writer.FlushAsync(timeout.Token).ConfigureAwait(false);
        if (!allowed)
            return;
        connection.SupportsExtendedFrames = (mode & Frames.ExtendedMode) != 0;
        if (credentials == 1)
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
        var operations = services.Require<BinaryDispatch>();
        using var deadlines = new BinaryRequestDeadlines(TimeProvider.System, connection.ConnectionClosed);
        var state = new CallState(connection, validator, operations, deadlines);
        var log = services.Require<IServerLoggerFactory>().CreateLogger<BinaryConnectionPump>();
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (frame, bytes, elapsed, predecessor) => ProcessAsync(state, frame, bytes, elapsed, predecessor),
            log,
            TimeProvider.System,
            connection.ConnectionClosed,
            connection.SupportsExtendedFrames ? services.Require<LargeCacheRequests>() : null);
        await pump.RunAsync(lifetime.Token).ConfigureAwait(false);
    }

    private static async ValueTask<BinaryResponse> ProcessAsync(CallState state, BinaryConnectionPump.RequestFrame frame, ReadOnlyMemory<byte> body, TimeSpan elapsed, Task predecessor)
    {
        var timeout = TimeSpan.FromMilliseconds(Math.Min(frame.Control & 0xffff, RpcAttemptTimeout.OperationDeadline.TotalMilliseconds)) - elapsed;
        if (timeout <= TimeSpan.Zero)
            return new BinaryResponse(FrameStatus.DeadlineExceeded);
        var operationId = Frames.ReadOperationId(frame.Control);
        var budget = new ServerDeadlineBudget(TimeProvider.System, timeout, TimeProvider.System.GetTimestamp());
        using var deadline = state.Deadlines.Start(timeout);
        var scope = state.Validator?.CreateRequestScope();
        try
        {
            var call = CreateRequestContext(state.Connection, scope?.ServiceProvider, operationId, deadline.Token);
            var failure = state.Validator == null ? FrameStatus.Ok : await state.Validator.AuthorizeAsync(state.Connection.Identity!, call, deadline.Token).ConfigureAwait(false);
            deadline.Token.ThrowIfCancellationRequested();
            if (failure != FrameStatus.Ok)
                return new BinaryResponse(failure);

            if (!TryReadTrace(call, frame.Control, ref body))
                return new BinaryResponse(FrameStatus.InvalidArgument);

            var callContext = new BinaryDispatch.Request(call, budget, predecessor, ValueBytes: frame.ValueBytes);
            return await state.Operations.ExecuteAsync(Unsafe.BitCast<int, BinaryOperation>(operationId), body, in callContext).ConfigureAwait(false);
        }
        catch (ServerCallException error)
        {
            return new BinaryResponse(error.StatusCode, error.Error);
        }
        catch (InvalidDataException)
        {
            return new BinaryResponse(FrameStatus.InvalidArgument);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !state.Connection.ConnectionClosed.IsCancellationRequested)
        {
            return new BinaryResponse(FrameStatus.DeadlineExceeded);
        }
        finally
        {
            if (scope is { } requestScope)
                await requestScope.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static BinaryRequestContext CreateRequestContext(BinaryConnection connection, IServiceProvider? requestServices, int operationId, CancellationToken cancellationToken) =>
        new()
        {
            Connection = connection,
            RequestServices = requestServices,
            RequestAborted = cancellationToken,
            Path = operationId < OperationPaths.Length ? OperationPaths[operationId] : OperationPaths[0],
        };

    private static bool TryReadTrace(BinaryRequestContext call, int control, ref ReadOnlyMemory<byte> body)
    {
        if ((control & Frames.TracedRequest) == 0)
            return true;

        var traced = BinaryTracedRequest.Parser.ParseBorrowed(body);
        if (traced.TraceParent.Length > Frames.MaxTraceParentLength || traced.TraceState.Length > Frames.MaxTraceStateLength)
            return false;

        call.TraceParent = traced.TraceParent;
        call.TraceState = traced.TraceState.Length == 0 ? null : traced.TraceState;
        body = traced.Payload;
        return true;
    }

    private sealed record CallState(
        BinaryConnection Connection,
        NativeConnectionAuthenticationValidator? Validator,
        BinaryDispatch Operations,
        BinaryRequestDeadlines Deadlines);
}
