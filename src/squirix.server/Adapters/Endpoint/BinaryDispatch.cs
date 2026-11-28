using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class BinaryDispatch
{
    private readonly RoutedCacheOperations<StoredValue> _operations;
    private readonly CacheRequestAdmission _admission;
    private readonly ServerCallCorrelation _correlation;
    private readonly BinaryCallObserver? _observer;

    internal BinaryDispatch(RoutedCacheOperations<StoredValue> operations, CacheRequestAdmission admission, ServerCallCorrelation correlation, BinaryCallObserver? observer = null)
    {
        _operations = operations;
        _admission = admission;
        _correlation = correlation;
        _observer = observer;
    }

    internal bool HasSoleOwner => _operations.Reads.HasSoleOwner;

    internal ValueTask<BinaryResponse> ExecuteAsync(BinaryOperation operation, ReadOnlyMemory<byte> body, in Request context) => operation switch
    {
        BinaryOperation.SetEntry => InvokeAsync(CacheOperationNames.Set, SetEntryAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.SetEntryAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.GetEntry => InvokeAsync(CacheOperationNames.GetEntry, GetEntryAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.GetEntryAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.GetValue => InvokeAsync(CacheOperationNames.Get, GetValueAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.GetValueAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.Remove => InvokeAsync(CacheOperationNames.Remove, RemoveAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.RemoveAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.TryAddEntry => InvokeAsync(CacheOperationNames.TryAdd, TryAddEntryAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.AddEntryIfAbsentAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.Touch => InvokeAsync(CacheOperationNames.Touch, TouchAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.TouchAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.RemoveExpiration => InvokeAsync(CacheOperationNames.RemoveExpiration, RemoveExpirationAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.RemoveExpirationAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.Update => InvokeAsync(CacheOperationNames.Update, UpdateAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.UpdateAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.GetExpiration => InvokeAsync(CacheOperationNames.GetEntry, GetExpirationAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.GetExpirationAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.GetOrAdd => InvokeAsync(CacheOperationNames.GetOrAdd, GetOrAddAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.GetOrAddAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.SetDistributed => InvokeDistributedSetAsync(body, in context),
        BinaryOperation.GetDistributed => InvokeAsync(CacheOperationNames.Get, GetDistributedAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.GetDistributedAsync(request, token, call.Deadline, call), body, in context),
        BinaryOperation.RefreshDistributed => InvokeAsync(CacheOperationNames.Touch, RefreshDistributedAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.RefreshDistributedAsync(request, token, call.Deadline, call), body, in context),
        _ => ValueTask.FromResult(new BinaryResponse(FrameStatus.Unimplemented, new BinaryCallError { Detail = "Unknown binary cache operation." })),
    };

    internal ValueTask<BinaryResponse> ExecuteOwnerAsync(PeerOperation peerOperation, BinaryOperation operation, ReadOnlyMemory<byte> body, in Request context) => peerOperation switch
    {
        PeerOperation.ReplaceEntryIfPresent => InvokeAsync(CacheOperationNames.Update, SetEntryAsyncRequest.Parser, static (ops, request, token, call) => ops.Mutations.ReplaceEntryIfPresentAsync(request, token, call.Deadline, call), body, in context),
        PeerOperation.GetRespValue => InvokeAsync(CacheOperationNames.Get, GetValueAsyncRequest.Parser, static (ops, request, token, call) => ops.Reads.GetRespValueAsync(request, token, call.Deadline, call), body, in context),
        _ => ExecuteAsync(operation, body, in context),
    };

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<BinaryResponse> DispatchAsync<TRequest, TResponse>(
        string operationName,
        TRequest request,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, BinaryRequestContext, ValueTask<TResponse>> method,
        Request context)
        where TResponse : WireMessage
    {
        var call = context.Call;
        if (context.ValueBytes is { } bytes && (request is not WireMessage message || CacheWritePayload.Length(message) != bytes))
            return new BinaryResponse(FrameStatus.InvalidArgument);

        // Preparation can overlap, but admission and execution must observe earlier mutations.
        if (!context.Predecessor.IsCompletedSuccessfully)
            await context.Predecessor.WaitAsync(call.RequestAborted).ConfigureAwait(false);
        call.RequestAborted.ThrowIfCancellationRequested();

        var path = call.Path;
        using var activity = ServerCallCorrelation.StartActivity(path, call.TraceParent, call.TraceState);
        call.LogScope = _correlation.CreateLogScope(path, activity);
        BinaryResponse response;
        try
        {
            call.Forwarded = context.Forwarded;
            call.ReportUnknownMutationOutcome = context.ReportUnknownMutationOutcome;
            response = await AdmitWithDeadlineAsync(operationName, request, method, context).ConfigureAwait(false);
        }
        catch (Exception error) when (error is ResourceExhaustedException or JournalCapacityExceededException or ServerOpIdMismatchException or SquirixException)
        {
            response = MapDispatchError(error);
        }

        _correlation.LogResult(call.LogScope, response.Status);
        return response;
    }

    private static BinaryResponse MapDispatchError(Exception error) => error switch
    {
        ResourceExhaustedException => BinaryResponse.FromError(ServerOpContract.MemoryPressure()),
        JournalCapacityExceededException => BinaryResponse.FromError(ServerOpContract.JournalDiskQuota()),
        ServerOpIdMismatchException => BinaryResponse.FromError(ServerOpContract.OperationIdReuseMismatch()),
        SquirixException failure => BinaryResponse.FromError(failure),
        _ => throw new ArgumentOutOfRangeException(nameof(error)),
    };

    private ValueTask<BinaryResponse> InvokeDistributedSetAsync(ReadOnlyMemory<byte> body, in Request context) =>
        DispatchAsync(
            CacheOperationNames.Set,
            SetDistributedAsyncRequest.Parser.ParseBorrowed(body, context.Call.Connection?.Names),
            static (ops, request, token, call) => ops.Mutations.SetDistributedAsync(request, token, call.Deadline, call),
            context);

    private ValueTask<BinaryResponse> InvokeAsync<TRequest, TResponse>(
        string operationName,
        WireParser<TRequest> parser,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, BinaryRequestContext, ValueTask<TResponse>> method,
        ReadOnlyMemory<byte> body,
        in Request context)
        where TRequest : WireMessage
        where TResponse : WireMessage =>
        DispatchAsync(operationName, body.Length > Frames.MaxRequestBytes ? parser.ParseBorrowed(body, context.Call.Connection?.Names) : parser.ParseFrom(body.Span, context.Call.Connection?.Names), method, context);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> AdmitWithDeadlineAsync<TRequest, TResponse>(
        string operationName,
        TRequest request,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, BinaryRequestContext, ValueTask<TResponse>> method,
        Request context)
        where TResponse : WireMessage
    {
        var call = context.Call;
        call.Deadline = context.Deadline;
        var (decision, lease) = await _admission.AcquireAsync(operationName, call, call.RequestAborted).ConfigureAwait(false);
        if (!decision.IsAccepted)
            return BinaryResponse.FromError(ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown"));

        using (lease)
            return await InvokeAdmittedAsync(call, request, method).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> InvokeAdmittedAsync<TRequest, TResponse>(
        BinaryRequestContext call,
        TRequest request,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, BinaryRequestContext, ValueTask<TResponse>> method)
        where TResponse : WireMessage
    {
        _admission.Grant(call);
        try
        {
            if (_observer != null)
                await _observer.EnterAsync(call).ConfigureAwait(false);
            var response = await method(_operations, request, call.RequestAborted, call).ConfigureAwait(false);
            return new BinaryResponse(FrameStatus.Ok, response);
        }
        finally
        {
            try
            {
                _observer?.Exit();
            }
            finally
            {
                call.SetAdmissionGate(null);
            }
        }
    }

    internal readonly record struct Request(BinaryRequestContext Call, ServerDeadlineBudget? Deadline, Task Predecessor, bool Forwarded = false, int? ValueBytes = null, bool ReportUnknownMutationOutcome = false);
}
