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
using Squirix.Transport.Grpc.Cache;
using Squirix.Utils;

namespace Squirix.Internal;

internal sealed class RemoteCache<T> : ICache<T>
{
    private readonly string _cacheName;
    private readonly IClientPool _clients;
    private readonly BootstrapEndpointFailover _failover;
    private readonly KeyedSingleFlight _getOrAddFlights = new();
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
        var entry = await GetEntryOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
        return entry is null ? new CacheEntryResult<T>(false, null) : new CacheEntryResult<T>(true, entry);
    }

    public async Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        var entry = await GetEntryOrDefaultAsync(key, cancellationToken).ConfigureAwait(false);
        if (entry is null)
            return new CacheExpirationResult(false, false, null);

        if (entry.ExpiresUtc is null)
            return new CacheExpirationResult(true, false, null);

        var remaining = entry.ExpiresUtc.Value - DateTime.UtcNow;
        return new CacheExpirationResult(true, true, remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
    }

    public async Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default)
    {
        var response = await ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetValueAsync(new GetValueRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetValueResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? new CacheValueResult<T>(true, ProtoEx.FromCacheValue<T>(response.Value, _serializer)) : new CacheValueResult<T>(false, default);
    }

    public Task<CacheValueResult<T>> GetOrAddAsync(
        string key,
        Func<string, CancellationToken, Task<T?>> valueFactory,
        CacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) =>
        CacheFacadeOps.GetOrAddAsync(this, _getOrAddFlights, key, valueFactory, options, cancellationToken);

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default) => await ExecuteAsync(
        async (client, ct) => (await client.RemoveAsync(new RemoveRequest { CacheName = _cacheName, Key = key }, cancellationToken: ct)).Removed,
        cancellationToken).ConfigureAwait(false);

    public async Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
        => await ExecuteAsync(
            async (client, ct) => (await client.RemoveExpirationAsync(new RemoveExpirationRequest { CacheName = _cacheName, Key = key }, cancellationToken: ct)).Found,
            cancellationToken).ConfigureAwait(false);

    public async Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        var entry = ToEntry(value, options);
        OperationInputValidator<T>.ValidateEntry(entry);

        _ = await ExecuteAsync(
            async (client, ct) => await client.SetValueAsync(ToSetValueRequest(key, entry), cancellationToken: ct),
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default) => await ExecuteAsync(
        async (client, ct) =>
        {
            ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(expiration));
            return (await client.TouchAsync(new TouchRequest { CacheName = _cacheName, Key = key, Expiration = Duration.FromTimeSpan(expiration) }, cancellationToken: ct)).Found;
        },
        cancellationToken).ConfigureAwait(false);

    public Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default)
    {
        var expiration = absoluteExpiration.UtcDateTime - DateTime.UtcNow;
        ExpirationInputValidator.ValidateRequiredPositive(expiration, nameof(absoluteExpiration));
        return TouchAsync(key, expiration, cancellationToken);
    }

    public Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default) =>
        CacheFacadeOps.UpdateAsync(this, key, value, cancellationToken);

    public async Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        var entry = ToEntry(value, options);
        OperationInputValidator<T>.ValidateEntry(entry);

        return await ExecuteAsync(
            async (client, ct) => (await client.TrySetValueAsync(ToTrySetValueRequest(key, entry), cancellationToken: ct)).Added,
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

    private SetValueRequest ToSetValueRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Value = ProtoEx.ToCacheValue(entry.Value, _serializer),
        ExpiresUtc = entry.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc)),
        Expiration = entry.Expiration is null ? null : Duration.FromTimeSpan(entry.Expiration.Value),
    };

    private TrySetValueRequest ToTrySetValueRequest(string key, CacheEntry<T> entry) => new()
    {
        CacheName = _cacheName,
        Key = key,
        Value = ProtoEx.ToCacheValue(entry.Value, _serializer),
        ExpiresUtc = entry.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc)),
        Expiration = entry.Expiration is null ? null : Duration.FromTimeSpan(entry.Expiration.Value),
    };

    private ValueTask<TResult> ExecuteAsync<TResult>(
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

    private ValueTask<TResult> ExecuteAsync<TState, TResult>(
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

    private async Task<CacheEntry<T>?> GetEntryOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteAsync(
                async (client, ct) =>
                    (await client.GetAsync(new GetRequest { CacheName = _cacheName, Key = key }, cancellationToken: ct)).Entry.MapProtoEntryToCacheEntry<T>(_serializer),
                cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return null;
        }
    }
}
