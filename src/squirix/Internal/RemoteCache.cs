using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Core;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

internal sealed class RemoteCache<T> : ICache<T>
{
    private readonly string _cacheName;
    private readonly KeyedSingleFlight<CacheValueResult<T>> _getOrAddFlights = new();
    private readonly RemoteCacheRpc _rpc;
    private readonly ISquirixSerializer _serializer;

    internal RemoteCache(string cacheName, EndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
    {
        _cacheName = CacheName.ParsePublic(cacheName).Canonical;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _rpc = new RemoteCacheRpc(_cacheName, failover, clients, serializer);
    }

    public async Task AddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (!await TryAddAsync(key, value, options, cancellationToken).ConfigureAwait(false))
            throw new CacheConflictException(key);
    }

    public async Task<CacheEntryResult<T>> GetEntryAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var entry = await GetEntryOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
        return entry is null ? new CacheEntryResult<T>(false, null) : new CacheEntryResult<T>(true, entry);
    }

    public async Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetExpirationAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        if (!response.Found)
            return new CacheExpirationResult(false, false, null);

        if (!response.HasExpiration)
            return new CacheExpirationResult(true, false, null);

        var remaining = response.Remaining.ToTimeSpan();
        return new CacheExpirationResult(true, true, remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
    }

    public Task<CacheValueResult<T>> GetOrAddAsync(
        string key,
        Func<string, CancellationToken, Task<T?>> valueFactory,
        CacheEntryOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ArgumentNullException.ThrowIfNull(valueFactory);

        return _getOrAddFlights.RunAsync(
            key,
            async ct =>
            {
                var created = await valueFactory(key, ct).ConfigureAwait(false);
                var entry = ToEntry(created, options);
                OperationInputValidator<T>.ValidateEntry(entry);

                var request = ToGetOrAddAsyncRequest(key, entry);
                request.OperationId = RpcOperationIdentity.New();
                var response = await ExecuteAsync(
                    static (client, state, token) =>
                    {
                        var responseAsync = client.GetOrAddAsync(state, cancellationToken: token).ResponseAsync;
                        return new ValueTask<GetOrAddAsyncResponse>(responseAsync);
                    },
                    request,
                    ct).ConfigureAwait(false);

                return new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, _serializer).ConfigureAwait(false));
            },
            cancellationToken);
    }

    public async Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetValueAsync(new GetValueAsyncRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetValueAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, _serializer).ConfigureAwait(false))
            : new CacheValueResult<T>(false, default);
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var operationId = RpcOperationIdentity.New();
        return await ExecuteAsync(
            async (client, ct) =>
                (await client.RemoveAsync(new RemoveAsyncRequest { OperationId = operationId, CacheName = _cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Removed,
            cancellationToken).ConfigureAwait(false);

        return response.Removed;
    }

    public async Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var operationId = RpcOperationIdentity.New();
        return await ExecuteAsync(
            async (client, ct) => (await client.RemoveExpirationAsync(
                new RemoveExpirationAsyncRequest { OperationId = operationId, CacheName = _cacheName, Key = key },
                cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Found,
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var entry = ToEntry(value, options);
        OperationInputValidator<T>.ValidateEntry(entry);
        var request = ToSetEntryAsyncRequest(key, entry);
        request.OperationId = RpcOperationIdentity.New();

        _ = await ExecuteAsync(async (client, ct) => await client.SetEntryAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        var response = await _rpc.ExecuteAsync(
            static (client, state, ct) =>
            {
                ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
                return (await client.TouchAsync(
                    new TouchAsyncRequest { OperationId = operationId, CacheName = _cacheName, Key = key, Expiration = Duration.FromTimeSpan(expiration) },
                    cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Found;
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New(), Expiration: Duration.FromTimeSpan(expiration)),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default)
    {
        var expiration = absoluteExpiration.UtcDateTime - DateTime.UtcNow;
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(absoluteExpiration));
        return TouchAsync(key, expiration, cancellationToken);
    }

    public async Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var entry = ToEntry(value, options);
        OperationInputValidator<T>.ValidateEntry(entry);
        var request = ToTryAddEntryAsyncRequest(key, entry);
        request.OperationId = RpcOperationIdentity.New();

        return await ExecuteAsync(async (client, ct) => (await client.TryAddEntryAsync(request, cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Added, cancellationToken)
           .ConfigureAwait(false);
    }

    public async Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var operationId = RpcOperationIdentity.New();
        return await ExecuteAsync(
            async (client, ct) => (await client.UpdateAsync(
                new UpdateAsyncRequest
                {
                    OperationId = operationId,
                    CacheName = _cacheName,
                    Key = key,
                    Entry = ProtoEx.MapEntryToProto(new CacheEntry<T> { Value = value }, _serializer),
                },
                cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Updated,
            cancellationToken).ConfigureAwait(false);
    }

    private static CacheEntry<T> ToEntry(T? value, CacheEntryOptions? options)
    {
        if (options?.Expiration is not null && options.ExpiresAt is not null)
            throw new ArgumentException("Cache entry options cannot specify both Expiration and ExpiresAt; set at most one expiration mechanism.", nameof(options));

        return new CacheEntry<T>
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = _cacheName,
            Key = key,
            Entry = ProtoEx.MapEntryToProto(new CacheEntry<T> { Value = value }, _serializer),
        };

        var response = await _rpc.ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.UpdateAsync(state, cancellationToken: ct).ResponseAsync;
                return new ValueTask<UpdateAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Updated;
    }

    private static async Task<CacheValueResult<T>> ExecuteGetOrAddAsync(GetOrAddFlightState state, CancellationToken cancellationToken)
    {
        var created = await state.ValueFactory(state.Key, cancellationToken).ConfigureAwait(false);
        var entry = RemoteCacheRpc.ToEntry(created, state.Options);
        OperationInputValidator.ValidateEntry(entry);

        var request = state.Cache._rpc.ToGetOrAddAsyncRequest(state.Key, entry);
        request.OperationId = RpcOperationIdentity.New();
        var response = await state.Cache._rpc.ExecuteAsync(
            static (client, requestState, token) =>
            {
                var responseAsync = client.GetOrAddAsync(requestState, cancellationToken: token).ResponseAsync;
                return new ValueTask<GetOrAddAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        return new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, state.Cache._serializer).ConfigureAwait(false));
    }

    private async Task<CacheEntry<T>?> GetEntryOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var response = await ExecuteAsync(
            async (client, ct) => await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = _cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? await response.Entry.MapProtoEntryToCacheEntryAsync<T>(_serializer).ConfigureAwait(false) : null;
    }

    private GetOrAddAsyncRequest ToGetOrAddAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };

    private SetEntryAsyncRequest ToSetEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };

    private TryAddEntryAsyncRequest ToTryAddEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Entry = ProtoEx.MapEntryToProto(entry, _serializer),
    };
}
