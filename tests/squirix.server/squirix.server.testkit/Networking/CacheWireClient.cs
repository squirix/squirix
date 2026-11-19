using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.TestKit.Networking;

internal readonly struct CacheWireClient
{
    private readonly PublicBinaryChannel _channel;

    internal CacheWireClient(PublicBinaryChannel channel)
    {
        _channel = channel;
    }

    internal Task<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.SetEntry, request, SetAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<GetEntryAsyncResponse> GetEntryAsync(GetEntryAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.GetEntry, request, GetEntryAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<GetValueAsyncResponse> GetValueAsync(GetValueAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.GetValue, request, GetValueAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<RemoveAsyncResponse> RemoveAsync(RemoveAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.Remove, request, RemoveAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(TryAddEntryAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.TryAddEntry, request, TryAddAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<TouchAsyncResponse> TouchAsync(TouchAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.Touch, request, TouchAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<RemoveExpirationAsyncResponse> RemoveExpirationAsync(RemoveExpirationAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.RemoveExpiration, request, RemoveExpirationAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<UpdateAsyncResponse> UpdateAsync(UpdateAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.Update, request, UpdateAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<GetExpirationAsyncResponse> GetExpirationAsync(GetExpirationAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.GetExpiration, request, GetExpirationAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<GetOrAddAsyncResponse> GetOrAddAsync(GetOrAddAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.GetOrAdd, request, GetOrAddAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<SetAsyncResponse> SetDistributedAsync(SetDistributedAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.SetDistributed, request, SetAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<GetDistributedAsyncResponse> GetDistributedAsync(GetDistributedAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.GetDistributed, request, GetDistributedAsyncResponse.Parser, deadline, cancellationToken);

    internal Task<RefreshDistributedAsyncResponse> RefreshDistributedAsync(RefreshDistributedAsyncRequest request, DateTime? deadline = null, CancellationToken cancellationToken = default) =>
        _channel.ExchangeAsync(BinaryOperation.RefreshDistributed, request, RefreshDistributedAsyncResponse.Parser, deadline, cancellationToken);
}
