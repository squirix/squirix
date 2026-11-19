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
        BinaryOperation.SetEntry => InvokeAsync(SetEntryAsyncRequest.Parser, static (ops, request, token) => ops.SetEntryAsync(request, token), body, context),
        BinaryOperation.GetEntry => InvokeAsync(GetEntryAsyncRequest.Parser, static (ops, request, token) => ops.GetEntryAsync(request, token), body, context),
        BinaryOperation.GetValue => InvokeAsync(GetValueAsyncRequest.Parser, static (ops, request, token) => ops.GetValueAsync(request, token), body, context),
        BinaryOperation.Remove => InvokeAsync(RemoveAsyncRequest.Parser, static (ops, request, token) => ops.RemoveAsync(request, token), body, context),
        BinaryOperation.TryAddEntry => InvokeAsync(TryAddEntryAsyncRequest.Parser, static (ops, request, token) => ops.AddEntryIfAbsentAsync(request, token), body, context),
        BinaryOperation.Touch => InvokeAsync(TouchAsyncRequest.Parser, static (ops, request, token) => ops.TouchAsync(request, token), body, context),
        BinaryOperation.RemoveExpiration => InvokeAsync(RemoveExpirationAsyncRequest.Parser, static (ops, request, token) => ops.RemoveExpirationAsync(request, token), body, context),
        BinaryOperation.Update => InvokeAsync(UpdateAsyncRequest.Parser, static (ops, request, token) => ops.UpdateAsync(request, token), body, context),
        BinaryOperation.GetExpiration => InvokeAsync(GetExpirationAsyncRequest.Parser, static (ops, request, token) => ops.GetExpirationAsync(request, token), body, context),
        BinaryOperation.GetOrAdd => InvokeAsync(GetOrAddAsyncRequest.Parser, static (ops, request, token) => ops.GetOrAddAsync(request, token), body, context),
        BinaryOperation.SetDistributed => InvokeAsync(SetDistributedAsyncRequest.Parser, static (ops, request, token) => ops.SetDistributedAsync(request, token), body, context),
        BinaryOperation.GetDistributed => InvokeAsync(GetDistributedAsyncRequest.Parser, static (ops, request, token) => ops.GetDistributedAsync(request, token), body, context),
        BinaryOperation.RefreshDistributed => InvokeAsync(RefreshDistributedAsyncRequest.Parser, static (ops, request, token) => ops.RefreshDistributedAsync(request, token), body, context),
        _ => ValueTask.FromResult(new BinaryResponse(FrameStatus.Unimplemented, new BinaryCallError { Detail = "Unknown binary cache operation." })),
    };

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<BinaryResponse> InvokeAsync<TRequest, TResponse>(
        MessageParser<TRequest> parser,
        Func<RoutedCacheOperations<object?>, TRequest, CancellationToken, Task<TResponse>> method,
        ReadOnlyMemory<byte> body,
        Request context)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        var request = parser.ParseFrom(body.Span);
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
