using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Adapters.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class BinaryDispatch
{
    private readonly SquirixServiceAdapter<object?> _adapter;
    private readonly CacheAdmissionInterceptor _admission;
    private readonly InboundCallInterceptor _inbound;
    private readonly BinaryCallObserver? _observer;

    internal BinaryDispatch(SquirixServiceAdapter<object?> adapter, CacheAdmissionInterceptor admission, InboundCallInterceptor inbound, BinaryCallObserver? observer)
    {
        _adapter = adapter;
        _admission = admission;
        _inbound = inbound;
        _observer = observer;
    }

    internal ValueTask<IMessage> ExecuteAsync(BinaryOperation operation, ReadOnlyMemory<byte> body, ServerCallContext context) => operation switch
    {
        BinaryOperation.SetEntry => InvokeAsync(SetEntryAsyncRequest.Parser, _adapter.SetEntry, body, context),
        BinaryOperation.GetEntry => InvokeAsync(GetEntryAsyncRequest.Parser, _adapter.GetEntry, body, context),
        BinaryOperation.GetValue => InvokeAsync(GetValueAsyncRequest.Parser, _adapter.GetValue, body, context),
        BinaryOperation.Remove => InvokeAsync(RemoveAsyncRequest.Parser, _adapter.Remove, body, context),
        BinaryOperation.TryAddEntry => InvokeAsync(TryAddEntryAsyncRequest.Parser, _adapter.TryAddEntry, body, context),
        BinaryOperation.Touch => InvokeAsync(TouchAsyncRequest.Parser, _adapter.Touch, body, context),
        BinaryOperation.RemoveExpiration => InvokeAsync(RemoveExpirationAsyncRequest.Parser, _adapter.RemoveExpiration, body, context),
        BinaryOperation.Update => InvokeAsync(UpdateAsyncRequest.Parser, _adapter.Update, body, context),
        BinaryOperation.GetExpiration => InvokeAsync(GetExpirationAsyncRequest.Parser, _adapter.GetExpiration, body, context),
        BinaryOperation.GetOrAdd => InvokeAsync(GetOrAddAsyncRequest.Parser, _adapter.GetOrAdd, body, context),
        BinaryOperation.SetDistributed => InvokeAsync(SetDistributedAsyncRequest.Parser, _adapter.SetDistributed, body, context),
        BinaryOperation.GetDistributed => InvokeAsync(GetDistributedAsyncRequest.Parser, _adapter.GetDistributed, body, context),
        BinaryOperation.RefreshDistributed => InvokeAsync(RefreshDistributedAsyncRequest.Parser, _adapter.RefreshDistributed, body, context),
        _ => ValueTask.FromException<IMessage>(new RpcException(new Status(StatusCode.Unimplemented, "Unknown binary cache operation."))),
    };

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IMessage> InvokeAsync<TRequest, TResponse>(
        MessageParser<TRequest> parser,
        UnaryServerMethod<TRequest, TResponse> method,
        ReadOnlyMemory<byte> body,
        ServerCallContext context)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        var handler = _observer == null ? method : ObserveCallAsync;
        return await _inbound.UnaryServerHandler(
            parser.ParseFrom(body.Span),
            context,
            (request, call) => _admission.UnaryServerHandler(request, call, handler)).ConfigureAwait(false);

        Task<TResponse> ObserveCallAsync(TRequest request, ServerCallContext callContext)
        {
            _observer.Observe(callContext);
            return method(request, callContext);
        }
    }
}
