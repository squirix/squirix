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

        return response.Found ? new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, _serializer).ConfigureAwait(false))
            : new CacheValueResult<T>(false, default);
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.RemoveAsync(
                    new RemoveAsyncRequest { OperationId = state.OperationId, CacheName = state.CacheName, Key = state.Key },
                    cancellationToken: ct).ResponseAsync;
                return new ValueTask<RemoveAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New()),
            cancellationToken).ConfigureAwait(false);

        return response.Removed;
    }

    public async Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.RemoveExpirationAsync(
                    new RemoveExpirationAsyncRequest { OperationId = state.OperationId, CacheName = state.CacheName, Key = state.Key },
                    cancellationToken: ct).ResponseAsync;
                return new ValueTask<RemoveExpirationAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New()),
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

        _ = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.SetEntryAsync(state, cancellationToken: ct).ResponseAsync;
                return new ValueTask<SetAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var touchAsyncRequest = new TouchAsyncRequest
                {
                    OperationId = state.OperationId,
                    CacheName = state.CacheName,
                    Key = state.Key,
                    Expiration = state.Expiration,
                };
                var responseAsync = client.TouchAsync(touchAsyncRequest, cancellationToken: ct).ResponseAsync;
                return new ValueTask<TouchAsyncResponse>(responseAsync);
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

        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.TryAddEntryAsync(state, cancellationToken: ct).ResponseAsync;
                return new ValueTask<TryAddAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Added;
    }

    public async Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = new UpdateAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = _cacheName,
            Key = key,
            Entry = ProtoEx.MapEntryToProto(new CacheEntry<T> { Value = value }, _serializer),
        };

        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.UpdateAsync(state, cancellationToken: ct).ResponseAsync;
                return new ValueTask<UpdateAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Updated;
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
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetEntryAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? await ProtoEx.MapProtoEntryToCacheEntryAsync<T>(response.Entry, _serializer).ConfigureAwait(false) : null;
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
