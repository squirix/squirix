using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
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
    private readonly IRemoteInvocationScopeFactory _scopes;

    internal BinaryDispatch(RoutedCacheOperations<StoredValue> operations, CacheRequestAdmission admission, ServerCallCorrelation correlation, IRemoteInvocationScopeFactory scopes, BinaryCallObserver? observer = null)
    {
        _operations = operations;
        _admission = admission;
        _correlation = correlation;
        _scopes = scopes;
        _observer = observer;
    }

    internal ValueTask<BinaryResponse> ExecuteAsync(BinaryOperation operation, ReadOnlyMemory<byte> body, in Request context) => operation switch
    {
        BinaryOperation.SetEntry => InvokeAsync(CacheOperationNames.Set, SetEntryAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.SetEntryAsync(request, token), body, in context),
        BinaryOperation.GetEntry => InvokeAsync(CacheOperationNames.GetEntry, GetEntryAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetEntryAsync(request, token), body, in context),
        BinaryOperation.GetValue => InvokeAsync(CacheOperationNames.Get, GetValueAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetValueAsync(request, token), body, in context),
        BinaryOperation.Remove => InvokeAsync(CacheOperationNames.Remove, RemoveAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.RemoveAsync(request, token), body, in context),
        BinaryOperation.TryAddEntry => InvokeAsync(CacheOperationNames.TryAdd, TryAddEntryAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.AddEntryIfAbsentAsync(request, token), body, in context),
        BinaryOperation.Touch => InvokeAsync(CacheOperationNames.Touch, TouchAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.TouchAsync(request, token), body, in context),
        BinaryOperation.RemoveExpiration => InvokeAsync(CacheOperationNames.RemoveExpiration, RemoveExpirationAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.RemoveExpirationAsync(request, token), body, in context),
        BinaryOperation.Update => InvokeAsync(CacheOperationNames.Update, UpdateAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.UpdateAsync(request, token), body, in context),
        BinaryOperation.GetExpiration => InvokeAsync(CacheOperationNames.GetEntry, GetExpirationAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetExpirationAsync(request, token), body, in context),
        BinaryOperation.GetOrAdd => InvokeAsync(CacheOperationNames.GetOrAdd, GetOrAddAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.GetOrAddAsync(request, token), body, in context),
        BinaryOperation.SetDistributed => InvokeDistributedSetAsync(body, in context),
        BinaryOperation.GetDistributed => InvokeAsync(CacheOperationNames.Get, GetDistributedAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetDistributedAsync(request, token), body, in context),
        BinaryOperation.RefreshDistributed => InvokeAsync(CacheOperationNames.Touch, RefreshDistributedAsyncRequest.Parser, static (ops, request, token) => ops.Reads.RefreshDistributedAsync(request, token), body, in context),
        _ => ValueTask.FromResult(new BinaryResponse(FrameStatus.Unimplemented, new BinaryCallError { Detail = "Unknown binary cache operation." })),
    };

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
            static (ops, request, token) => ops.Mutations.SetDistributedAsync(request, token),
            context);

    private ValueTask<BinaryResponse> InvokeAsync<TRequest, TResponse>(
        string operationName,
        WireParser<TRequest> parser,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, ValueTask<TResponse>> method,
        ReadOnlyMemory<byte> body,
        in Request context)
        where TRequest : WireMessage
        where TResponse : WireMessage =>
        DispatchAsync(operationName, body.Length > Frames.MaxRequestBytes ? parser.ParseBorrowed(body, context.Call.Connection?.Names) : parser.ParseFrom(body.Span, context.Call.Connection?.Names), method, context);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> DispatchAsync<TRequest, TResponse>(
        string operationName,
        TRequest request,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, ValueTask<TResponse>> method,
        Request context)
        where TRequest : WireMessage
        where TResponse : WireMessage
    {
        var call = context.Call;
        if (context.ValueBytes is { } bytes && CacheWritePayload.Length(request) != bytes)
            return new BinaryResponse(FrameStatus.InvalidArgument);

        // Preparation can overlap, but admission and execution must observe earlier mutations.
        if (!context.Predecessor.IsCompletedSuccessfully)
            await context.Predecessor.WaitAsync(call.RequestAborted).ConfigureAwait(false);
        call.RequestAborted.ThrowIfCancellationRequested();

        var path = call.Path;
        try
        {
            using var invocation = _scopes.EnterRemoteInvocation(context.Forwarded);
            using var activity = ServerCallCorrelation.StartActivity(path, call.TraceParent, call.TraceState);
            using var log = _correlation.BeginLogScope(path);
            using var deadline = context.Forwarded ? null : ServerRpcDeadlineContext.Push(context.Deadline);
            var (decision, lease) = await _admission.AcquireAsync(operationName, call.RequestAborted).ConfigureAwait(false);
            if (!decision.IsAccepted)
                return BinaryResponse.FromError(ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown"));

            using (lease)
                return await InvokeAdmittedAsync(call, request, method).ConfigureAwait(false);
        }
        catch (Exception error) when (error is ResourceExhaustedException or JournalCapacityExceededException or ServerOpIdMismatchException or SquirixException)
        {
            return MapDispatchError(error);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> InvokeAdmittedAsync<TRequest, TResponse>(
        BinaryRequestContext call,
        TRequest request,
        Func<RoutedCacheOperations<StoredValue>, TRequest, CancellationToken, ValueTask<TResponse>> method)
        where TRequest : WireMessage
        where TResponse : WireMessage
    {
        using var scope = _admission.EnterDispatchScope();
        try
        {
            if (_observer != null)
                await _observer.EnterAsync(call).ConfigureAwait(false);
            var response = await method(_operations, request, call.RequestAborted).ConfigureAwait(false);
            return new BinaryResponse(FrameStatus.Ok, response);
        }
        finally
        {
            _observer?.Exit();
        }
    }

    internal readonly record struct Request(BinaryRequestContext Call, ServerDeadlineBudget Deadline, Task Predecessor, bool Forwarded = false, int? ValueBytes = null);
}
