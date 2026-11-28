using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal;

[Immutable]
internal sealed class RemoteCache<T> : ICache<T>
{
    private readonly string _cacheName;
    private readonly RemoteCacheRpc _rpc;
    private readonly ISquirixSerializer _serializer;
    private KeyedSingleFlight<T?, CacheValueResult<T>>? _getOrAddFlights;

    internal RemoteCache(string cacheName, EndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
    {
        _cacheName = CacheName.ParsePublic(cacheName).Canonical;
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
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
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options),
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        if (!response.Found)
            return new CacheEntryResult<T>(false, null);

        // The server reports the duration remaining so the client can use its own clock for the deadline.
        DateTime? deadline = response.Remaining is { } remaining ? _rpc.RebaseDeadline(remaining) : null;
        var entry = WireMapping.MapWireEntryToCacheEntry<T>(response.Entry, _serializer, deadline);
        return new CacheEntryResult<T>(true, entry);
    }

    public async Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options),
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        if (!response.Found)
            return new CacheExpirationResult(false, false, null);

        if (!response.HasExpiration)
            return new CacheExpirationResult(true, false, null);

        var remaining = WireMapping.ReadDuration(response.Remaining);
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

        // Options that can never be written are refused before the factory runs; the lifetime is still measured from to send.
        _ = _rpc.ResolveExpiration(options);
        var flights = LazyInitializer.EnsureInitialized(ref _getOrAddFlights, static () => new());
        return flights.RunAsync(
            key,
            new GetOrAddFlightState(this, key, valueFactory, options),
            static (state, ct) => state.ValueFactory(state.Key, ct),
            static (state, created, ct) => SendGetOrAddAsync(state, created, ct),
            cancellationToken);
    }

    public async Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var r = await _rpc.ExecuteAsync(
            static (client, state, options) => client.GetValueAsync(new GetValueAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options),
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return r.Found ? new CacheValueResult<T>(true, WireMapping.Read<T>(r.Value, _serializer)) : new CacheValueResult<T>(false, default);
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = new RemoveAsyncRequest { CacheName = _cacheName, Key = key };
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.RemoveAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Removed;
    }

    public async Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = new RemoveExpirationAsyncRequest { CacheName = _cacheName, Key = key };
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.RemoveExpirationAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = _rpc.ToSetEntryAsyncRequest(key, value);
        request.Entry = _rpc.WithExpiration(request.Entry, options);

        _ = await _rpc.ExecuteAsync(
            static (client, state, options) => client.SetEntryAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        var request = new TouchAsyncRequest { CacheName = _cacheName, Key = key, Expiration = expiration };
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.TouchAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default)
    {
        var expiration = absoluteExpiration - _rpc.Clock.GetUtcNow();
        expiration.ThrowIfNegativeOrZero(nameof(absoluteExpiration), "expiration must be greater than zero.");
        return TouchAsync(key, expiration, cancellationToken);
    }

    public async Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = _rpc.ToTryAddEntryAsyncRequest(key, value);
        request.Entry = _rpc.WithExpiration(request.Entry, options);

        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.AddEntryIfMissingAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Added;
    }

    public async Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = new UpdateAsyncRequest
        {
            CacheName = _cacheName,
            Key = key,
            Entry = WireMapping.CreateWireEntry(value, _serializer),
        };

        var response = await _rpc.ExecuteAsync(
            static (client, state, options) => client.UpdateAsync(state, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Updated;
    }

    private static async Task<CacheValueResult<T>> SendGetOrAddAsync(GetOrAddFlightState state, T? created, CancellationToken cancellationToken)
    {
        var cache = state.Cache;
        var request = cache._rpc.ToGetOrAddAsyncRequest(state.Key, created);
        request.Entry = cache._rpc.WithExpiration(request.Entry, state.Options);
        var r = await cache._rpc.ExecuteAsync(
            static (client, requestState, options) => client.GetOrAddAsync(requestState, in options),
            request,
            cancellationToken).ConfigureAwait(false);

        // An add answers without the value: the caller already holds the instance it created.
        return (r.Added, r.Found) switch
        {
            (true, _) => new CacheValueResult<T>(true, created),
            (false, true) => new CacheValueResult<T>(true, WireMapping.Read<T>(r.Value, cache._serializer)),
            _ => new CacheValueResult<T>(false, default),
        };
    }

    [Immutable]
    private sealed record GetOrAddFlightState(RemoteCache<T> Cache, string Key, Func<string, CancellationToken, Task<T?>> ValueFactory, CacheEntryOptions? Options);

    [Immutable]
    internal sealed class RemoteCacheRpc
    {
        private readonly string _cacheName;
        private readonly IClientPool _clients;
        private readonly EndpointFailover _failover;
        private readonly ISquirixSerializer _serializer;

        internal RemoteCacheRpc(string cacheName, EndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
        {
            _cacheName = cacheName;
            ArgumentNullException.ThrowIfNull(failover);
            ArgumentNullException.ThrowIfNull(clients);
            ArgumentNullException.ThrowIfNull(serializer);
            _failover = failover;
            _clients = clients;
            _serializer = serializer;
        }

        /// <summary>Gets the client clock absolute expirations are given on.</summary>
        internal TimeProvider Clock => _failover.Clock;

        /// <summary>
        /// Sets the expiration of an already serialized wire entry, measured now on the client clock, so the time spent serializing the value
        /// does not stretch the lifetime; the request is then sent unchanged on every retry.
        /// </summary>
        /// <param name="entry">The wire entry, value already serialized.</param>
        /// <param name="options">The entry options.</param>
        /// <exception cref="ArgumentException">Both an absolute and a relative expiration are set.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The absolute expiration is not in the future on the client clock.</exception>
        /// <returns>The entry with its resolved expiration.</returns>
        internal CacheEntryWire? WithExpiration(CacheEntryWire? entry, CacheEntryOptions? options) =>
            ResolveExpiration(options) is { } expiration && entry is { } present ? present with { Expiration = expiration } : entry;

        /// <summary>Returns the relative expiration to send for <paramref name="options" />, measured now on the client clock.</summary>
        /// <param name="options">The entry options.</param>
        /// <returns>The relative expiration, or <see langword="null" /> for none.</returns>
        /// <exception cref="ArgumentException">Both an absolute and a relative expiration are set.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The absolute expiration is not in the future on the client clock.</exception>
        internal TimeSpan? ResolveExpiration(CacheEntryOptions? options)
        {
            const string message = "Cache entry options cannot specify both Expiration and ExpiresAt; set at most one expiration mechanism.";
            if (options is { Expiration: not null, ExpiresAt: not null })
                throw new ArgumentException(message, nameof(options));

            if (options?.ExpiresAt is not { } expiresAt)
                return options?.Expiration;

            var remaining = expiresAt - Clock.GetUtcNow();
            remaining.ThrowIfNegativeOrZero(nameof(options), "ExpiresAt must be in the future on the client clock.");
            return remaining;
        }

        /// <summary>Turns the time an entry has left into a deadline on the client clock, saturating at the largest date.</summary>
        /// <param name="remaining">The time left reported by the server.</param>
        /// <returns>The deadline on the client clock.</returns>
        internal DateTime RebaseDeadline(TimeSpan remaining)
        {
            var now = Clock.GetUtcNow();
            return remaining >= DateTimeOffset.MaxValue - now ? DateTime.MaxValue : (now + remaining).UtcDateTime;
        }

        internal ValueTask<TResult> ExecuteAsync<TState, TResult>(
            Func<BinaryCacheClient, TState, BinaryCallOptions, ValueTask<TResult>> action,
            TState state,
            CancellationToken cancellationToken)
        {
            if (_failover.SoleEndpoint is not { } node)
                return ExecuteWithFailoverAsync(action, state, cancellationToken);

            ValueTask<TResult> operation;
            try
            {
                operation = ExecuteOnNodeAsync(node, action, state, _failover.BeginOperation(node), cancellationToken, RemoteRpcErrorMapper.Map);
            }
            catch (SquirixTransportException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }

            return operation;
        }

        internal GetOrAddAsyncRequest ToGetOrAddAsyncRequest(string key, T? value) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = WireMapping.CreateWireEntry(value, _serializer),
        };

        internal SetEntryAsyncRequest ToSetEntryAsyncRequest(string key, T? value) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = WireMapping.CreateWireEntry(value, _serializer),
        };

        internal TryAddEntryAsyncRequest ToTryAddEntryAsyncRequest(string key, T? value) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = WireMapping.CreateWireEntry(value, _serializer),
        };

        private async ValueTask<TResult> ExecuteWithFailoverAsync<TState, TResult>(
            Func<BinaryCacheClient, TState, BinaryCallOptions, ValueTask<TResult>> action,
            TState state,
            CancellationToken cancellationToken)
        {
            try
            {
                return await _failover.ExecuteAsync(
                    static (node, execution, budget, ct) => execution.Cache.ExecuteOnNodeAsync(node, execution.Action, execution.State, in budget, ct),
                    (Cache: this, Action: action, State: state),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (SquirixTransportException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }
        }

        private ValueTask<TResult> ExecuteOnNodeAsync<TState, TResult>(
            string nodeId, Func<BinaryCacheClient, TState, BinaryCallOptions, ValueTask<TResult>> action, TState state, in DeadlineBudget budget, CancellationToken cancellationToken, Action<SquirixTransportException>? onFailure = null)
        {
            var node = _clients.ForNode(nodeId);
            return node.Policy.ExecuteTransportAsync(
                static (execution, token, expires) => execution.Action(
                    execution.Client, execution.State, new BinaryCallOptions(execution.Budget.ForwardDeadlineUtc, token, execution.CallerToken, expires)),
                (Action: action, node.Client, State: state, Budget: budget, CallerToken: cancellationToken),
                budget,
                onFailure,
                cancellationToken);
        }
    }
}
