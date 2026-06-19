using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Core;
using Squirix.Internal.Cluster.Bootstrap;
using Squirix.Internal.Cluster.Transport;
using Squirix.Internal.Decorators.Validation;
using Squirix.Serialization;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using Squirix.Utils;

namespace Squirix.Internal;

internal sealed class RemoteCache<T> : ICache<T>
{
    private readonly string _cacheName;
    private readonly IClientPool _clients;
    private readonly BootstrapEndpointFailover _failover;
    private readonly KeyedSingleFlight<CacheValueResult<T>> _getOrAddFlights = new();
    private readonly ISquirixSerializer _serializer;

    public RemoteCache(string cacheName, BootstrapEndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
    {
        _cacheName = CacheName.ParsePublic(cacheName).Canonical;
        _failover = failover ?? throw new ArgumentNullException(nameof(failover));
        _clients = clients ?? throw new ArgumentNullException(nameof(clients));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
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
        var response = await ExecuteAsync(
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
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetValueAsync(new GetValueAsyncRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetValueAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return response.Found
            ? new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, _serializer).ConfigureAwait(false))
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
        var operationId = RpcOperationIdentity.New();
        return await ExecuteAsync(
            async (client, ct) =>
            {
                ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
                return (await client.TouchAsync(
                    new TouchAsyncRequest { OperationId = operationId, CacheName = _cacheName, Key = key, Expiration = Duration.FromTimeSpan(expiration) },
                    cancellationToken: ct).ResponseAsync.ConfigureAwait(false)).Found;
            },
            cancellationToken).ConfigureAwait(false);
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
            Value = value,
            Expiration = options?.Expiration,
            ExpiresUtc = options?.ExpiresAt?.UtcDateTime,
        };
    }

    private ValueTask<TResult> ExecuteAsync<TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) => ExecuteMappedAsync(
        static async (cache, state, ct) =>
        {
            try
            {
                return await cache.ExecuteCoreAsync(state.Action, ct).ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }
        },
        (Cache: this, Action: action),
        cancellationToken);

    private ValueTask<TResult> ExecuteAsync<TState, TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken) => ExecuteMappedAsync(
        static async (cache, execution, ct) =>
        {
            try
            {
                return await cache.ExecuteCoreAsync(execution.Action, execution.State, ct).ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }
        },
        (Cache: this, Action: action, State: state),
        cancellationToken);

    private ValueTask<TResult> ExecuteCoreAsync<TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) => _failover.ExecuteAsync(
        static (nodeId, state, ct) =>
        {
            var client = state.Cache._clients.ForNode(nodeId);
            return state.Cache._clients.PolicyFor(nodeId).ExecuteAsync(
                static (policyState, token) => policyState.Action(policyState.Client, token),
                (state.Action, Client: client),
                ct);
        },
        (Cache: this, Action: action),
        cancellationToken);

    private ValueTask<TResult> ExecuteCoreAsync<TState, TResult>(
        Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken) => _failover.ExecuteAsync(
        static (nodeId, execution, ct) =>
        {
            var client = execution.Cache._clients.ForNode(nodeId);
            return execution.Cache._clients.PolicyFor(nodeId).ExecuteAsync(
                static (policyState, token) => policyState.Action(policyState.Client, policyState.State, token),
                (execution.Action, Client: client, execution.State),
                ct);
        },
        (Cache: this, Action: action, State: state),
        cancellationToken);

    private ValueTask<TResult> ExecuteMappedAsync<TState, TResult>(
        Func<RemoteCache<T>, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken) => _failover.ExecuteAsync(
        (_, execution, ct) => action(execution.Cache, execution.State, ct),
        (Cache: this, State: state),
        cancellationToken);

    private async Task<CacheEntry<T>?> GetEntryOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteAsync(
                async (client, ct) =>
                {
                    var response = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = _cacheName, Key = key }, cancellationToken: ct).ResponseAsync.ConfigureAwait(false);
                    return await response.Entry.MapProtoEntryToCacheEntryAsync<T>(_serializer).ConfigureAwait(false);
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.NotFound)
        {
            return null;
        }
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
