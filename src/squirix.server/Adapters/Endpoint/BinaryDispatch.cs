using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class BinaryDispatch
{
    private readonly RoutedCacheOperations<object?> _operations;
    private readonly CacheRequestAdmission _admission;
    private readonly ServerCallCorrelation _correlation;
    private readonly BinaryCallObserver? _observer;
    private readonly IRemoteInvocationScopeFactory _scopes;

    public BinaryDispatch(RoutedCacheOperations<object?> operations, CacheRequestAdmission admission, ServerCallCorrelation correlation, IRemoteInvocationScopeFactory scopes, BinaryCallObserver? observer = null)
    {
        _operations = operations;
        _admission = admission;
        _correlation = correlation;
        _scopes = scopes;
        _observer = observer;
    }

    internal ValueTask<BinaryResponse> ExecuteAsync(BinaryOperation operation, ReadOnlyMemory<byte> body, in Request context) => operation switch
    {
        BinaryOperation.SetEntry => InvokeAsync(SetEntryAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.SetEntryAsync(request, token), body, in context),
        BinaryOperation.GetEntry => InvokeAsync(GetEntryAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetEntryAsync(request, token), body, in context),
        BinaryOperation.GetValue => InvokeAsync(GetValueAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetValueAsync(request, token), body, in context),
        BinaryOperation.Remove => InvokeAsync(RemoveAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.RemoveAsync(request, token), body, in context),
        BinaryOperation.TryAddEntry => InvokeAsync(TryAddEntryAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.AddEntryIfAbsentAsync(request, token), body, in context),
        BinaryOperation.Touch => InvokeAsync(TouchAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.TouchAsync(request, token), body, in context),
        BinaryOperation.RemoveExpiration => InvokeAsync(RemoveExpirationAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.RemoveExpirationAsync(request, token), body, in context),
        BinaryOperation.Update => InvokeAsync(UpdateAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.UpdateAsync(request, token), body, in context),
        BinaryOperation.GetExpiration => InvokeAsync(GetExpirationAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetExpirationAsync(request, token), body, in context),
        BinaryOperation.GetOrAdd => InvokeAsync(GetOrAddAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.GetOrAddAsync(request, token), body, in context),
        BinaryOperation.SetDistributed => InvokeDistributedSetAsync(body, in context),
        BinaryOperation.GetDistributed => InvokeAsync(GetDistributedAsyncRequest.Parser, static (ops, request, token) => ops.Reads.GetDistributedAsync(request, token), body, in context),
        BinaryOperation.RefreshDistributed => InvokeAsync(RefreshDistributedAsyncRequest.Parser, static (ops, request, token) => ops.Reads.RefreshDistributedAsync(request, token), body, in context),
        _ => ValueTask.FromResult(new BinaryResponse(FrameStatus.Unimplemented, new BinaryCallError { Detail = "Unknown binary cache operation." })),
    };

    private ValueTask<BinaryResponse> InvokeDistributedSetAsync(ReadOnlyMemory<byte> body, in Request context) =>
        LargeDistributedSet.TryParse(body, out var metadata, out var value)
            ? DispatchAsync(
                (Metadata: metadata, Value: value),
                static (ops, state, token) => ops.Mutations.SetDistributedAsync(state.Metadata, state.Value, token),
                context)
            : InvokeAsync(SetDistributedAsyncRequest.Parser, static (ops, request, token) => ops.Mutations.SetDistributedAsync(request, token), body, in context);

    private ValueTask<BinaryResponse> InvokeAsync<TRequest, TResponse>(
        MessageParser<TRequest> parser,
        Func<RoutedCacheOperations<object?>, TRequest, CancellationToken, Task<TResponse>> method,
        ReadOnlyMemory<byte> body,
        in Request context)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse> =>
        DispatchAsync(parser.ParseFrom(body.Span), method, context);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> DispatchAsync<TRequest, TResponse>(
        TRequest request,
        Func<RoutedCacheOperations<object?>, TRequest, CancellationToken, Task<TResponse>> method,
        Request context)
        where TResponse : class, IMessage<TResponse>
    {
        var http = context.Http;

        // Preparation can overlap, but admission and execution must observe earlier mutations.
        if (!context.Predecessor.IsCompletedSuccessfully)
            await context.Predecessor.WaitAsync(http.RequestAborted).ConfigureAwait(false);
        http.RequestAborted.ThrowIfCancellationRequested();

        var path = http.Request.Path.Value!;
        try
        {
            using var invocation = _scopes.EnterRemoteInvocation(context.Forwarded);
            using var activity = ServerCallCorrelation.StartActivity(path, context.TraceParent, context.TraceState);
            using var log = _correlation.BeginLogScope(path);
            using var deadline = context.Forwarded ? null : ServerRpcDeadlineContext.Push(context.Deadline, TimeProvider.System);
            var (decision, lease) = await _admission.AcquireAsync(path, http.RequestAborted).ConfigureAwait(false);
            if (!decision.IsAccepted)
                return BinaryResponse.FromError(ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown"));

            using (lease)
            using (_admission.Enter())
            {
                _observer?.Observe(http);
                var response = await method(_operations, request, http.RequestAborted).ConfigureAwait(false);
                return new BinaryResponse(FrameStatus.Ok, response);
            }
        }
        catch (ResourceExhaustedException)
        {
            return BinaryResponse.FromError(ServerOpContract.MemoryPressure());
        }
        catch (JournalCapacityExceededException)
        {
            return BinaryResponse.FromError(ServerOpContract.JournalDiskQuota());
        }
        catch (ServerOpIdMismatchException)
        {
            return BinaryResponse.FromError(ServerOpContract.OperationIdReuseMismatch());
        }
        catch (SquirixException error)
        {
            return BinaryResponse.FromError(error);
        }
    }

    internal readonly record struct Request(HttpContext Http, DateTime Deadline, string? TraceParent, string? TraceState, Task Predecessor, bool Forwarded = false);
}
