using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

[Immutable]
internal sealed class SquirixServiceAdapter<T> : SquirixCacheService.SquirixCacheServiceBase
{
    private readonly RoutedCacheOperations<T> _operations;

    public SquirixServiceAdapter(RoutedCacheOperations<T> operations)
    {
        _operations = operations;
    }

    public override Task<GetDistributedAsyncResponse> GetDistributed(GetDistributedAsyncRequest request, ServerCallContext context) =>
        _operations.GetDistributedAsync(request, context.CancellationToken);

    public override Task<GetEntryAsyncResponse> GetEntry(GetEntryAsyncRequest request, ServerCallContext context) =>
        _operations.GetEntryAsync(request, context.CancellationToken);

    public override Task<GetExpirationAsyncResponse> GetExpiration(GetExpirationAsyncRequest request, ServerCallContext context) =>
        _operations.GetExpirationAsync(request, context.CancellationToken);

    public override Task<GetOrAddAsyncResponse> GetOrAdd(GetOrAddAsyncRequest request, ServerCallContext context) =>
        _operations.GetOrAddAsync(request, context.CancellationToken);

    public override Task<GetValueAsyncResponse> GetValue(GetValueAsyncRequest request, ServerCallContext context) =>
        _operations.GetValueAsync(request, context.CancellationToken);

    public override Task<RefreshDistributedAsyncResponse> RefreshDistributed(RefreshDistributedAsyncRequest request, ServerCallContext context) =>
        _operations.RefreshDistributedAsync(request, context.CancellationToken);

    public override Task<RemoveAsyncResponse> Remove(RemoveAsyncRequest request, ServerCallContext context) =>
        _operations.RemoveAsync(request, context.CancellationToken);

    public override Task<RemoveExpirationAsyncResponse> RemoveExpiration(RemoveExpirationAsyncRequest request, ServerCallContext context) =>
        _operations.RemoveExpirationAsync(request, context.CancellationToken);

    public override Task<SetAsyncResponse> SetDistributed(SetDistributedAsyncRequest request, ServerCallContext context) =>
        _operations.SetDistributedAsync(request, context.CancellationToken);

    public override Task<SetAsyncResponse> SetEntry(SetEntryAsyncRequest request, ServerCallContext context) =>
        _operations.SetEntryAsync(request, context.CancellationToken);

    public override Task<TouchAsyncResponse> Touch(TouchAsyncRequest request, ServerCallContext context) =>
        _operations.TouchAsync(request, context.CancellationToken);

    public override Task<TryAddAsyncResponse> TryAddEntry(TryAddEntryAsyncRequest request, ServerCallContext context) =>
        _operations.AddEntryIfAbsentAsync(request, context.CancellationToken);

    public override Task<UpdateAsyncResponse> Update(UpdateAsyncRequest request, ServerCallContext context) =>
        _operations.UpdateAsync(request, context.CancellationToken);
}
