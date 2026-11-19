using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal;

[Immutable]
internal sealed class RemoteCache<T> : ICache<T>
{
    private readonly string _cacheName;
    private readonly KeyedSingleFlight<T?, CacheValueResult<T>> _getOrAddFlights = new();
    private readonly RemoteCacheRpc _rpc;
    private readonly ISquirixSerializer _serializer;

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
            static (client, state, options) =>
            {
                var responseAsync = client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options)
;
                return new ValueTask<GetEntryAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        if (!response.Found)
            return new CacheEntryResult<T>(false, null);

        var entry = ProtoEx.MapProtoEntryToCacheEntry<T>(response.Entry, _serializer);

        // The server reports how long the entry has left; rebase it on the client clock so the deadline reads against it.
        var resultEntry = response.Remaining is { } remaining
            ? new CacheEntry<T> { Value = entry.Value, Expiration = entry.Expiration, ExpiresUtc = _rpc.RebaseDeadline(ProtoEx.ReadDuration(remaining)) }
            : entry;
        return new CacheEntryResult<T>(true, resultEntry);
    }

    public async Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options)
;
                return new ValueTask<GetExpirationAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        if (!response.Found)
            return new CacheExpirationResult(false, false, null);

        if (!response.HasExpiration)
            return new CacheExpirationResult(true, false, null);

        var remaining = ProtoEx.ReadDuration(response.Remaining);
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
        return _getOrAddFlights.RunAsync(
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
            static (client, state, options) =>
            {
                var responseAsync = client.GetValueAsync(new GetValueAsyncRequest { CacheName = state.CacheName, Key = state.Key }, in options)
;
                return new ValueTask<GetValueAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return r.Found ? new CacheValueResult<T>(true, ProtoEx.Read<T>(r.Value, _serializer)) : new CacheValueResult<T>(false, default);
    }

    public async Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.RemoveAsync(
                    new RemoveAsyncRequest { OperationId = state.OperationId, CacheName = state.CacheName, Key = state.Key },
                    in options);
                return new ValueTask<RemoveAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New()),
            cancellationToken).ConfigureAwait(false);

        return response.Removed;
    }

    public async Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.RemoveExpirationAsync(
                    new RemoveExpirationAsyncRequest { OperationId = state.OperationId, CacheName = state.CacheName, Key = state.Key },
                    in options);
                return new ValueTask<RemoveExpirationAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New()),
            cancellationToken).ConfigureAwait(false);

        return response.Found;
    }

    public async Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        var request = _rpc.ToSetEntryAsyncRequest(key, new CacheEntry<T> { Value = value });
        _rpc.ApplyExpiration(request.Entry, options);
        request.OperationId = RpcOperationIdentity.New();

        _ = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.SetEntryAsync(state, in options);
                return new ValueTask<SetAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
    {
        KeyInputValidator.Validate(key, nameof(key));
        expiration.ThrowIfNegativeOrZero(nameof(expiration), "expiration must be greater than zero.");
        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var touchAsyncRequest = new TouchAsyncRequest
                {
                    OperationId = state.OperationId,
                    CacheName = state.CacheName,
                    Key = state.Key,
                    Expiration = state.Expiration,
                };
                var responseAsync = client.TouchAsync(touchAsyncRequest, in options);
                return new ValueTask<TouchAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key, OperationId: RpcOperationIdentity.New(), Expiration: ProtoDuration.FromTimeSpan(expiration)),
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
        var request = _rpc.ToTryAddEntryAsyncRequest(key, new CacheEntry<T> { Value = value });
        _rpc.ApplyExpiration(request.Entry, options);
        request.OperationId = RpcOperationIdentity.New();

        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.AddEntryIfMissingAsync(state, in options);
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

        var response = await _rpc.ExecuteAsync(
            static (client, state, options) =>
            {
                var responseAsync = client.UpdateAsync(state, in options);
                return new ValueTask<UpdateAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        return response.Updated;
    }

    private static async Task<CacheValueResult<T>> SendGetOrAddAsync(GetOrAddFlightState state, T? created, CancellationToken cancellationToken)
    {
        var cache = state.Cache;
        var request = cache._rpc.ToGetOrAddAsyncRequest(state.Key, new CacheEntry<T> { Value = created });
        cache._rpc.ApplyExpiration(request.Entry, state.Options);
        request.OperationId = RpcOperationIdentity.New();
        var r = await cache._rpc.ExecuteAsync(
            static (client, requestState, options) =>
            {
                var responseAsync = client.GetOrAddAsync(requestState, in options);
                return new ValueTask<GetOrAddAsyncResponse>(responseAsync);
            },
            request,
            cancellationToken).ConfigureAwait(false);

        // An add answers without the value: the caller already holds the instance it created.
        return (r.Added, r.Found) switch
        {
            (true, _) => new CacheValueResult<T>(true, created),
            (false, true) => new CacheValueResult<T>(true, ProtoEx.Read<T>(r.Value, cache._serializer)),
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
        internal void ApplyExpiration(CacheEntryWire entry, CacheEntryOptions? options)
        {
            if (ResolveExpiration(options) is { } expiration)
                entry.Expiration = ProtoDuration.FromTimeSpan(expiration);
        }

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
            CancellationToken cancellationToken) => AwaitRpcGuardedAsync(ExecuteCoreAsync(action, state, cancellationToken));

        internal GetOrAddAsyncRequest ToGetOrAddAsyncRequest(string key, CacheEntry<T> entry) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = ProtoEx.MapEntryToProto(entry, _serializer),
        };

        internal SetEntryAsyncRequest ToSetEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = ProtoEx.MapEntryToProto(entry, _serializer),
        };

        internal TryAddEntryAsyncRequest ToTryAddEntryAsyncRequest(string key, CacheEntry<T> entry) => new()
        {
            CacheName = _cacheName,
            Key = key,
            Entry = ProtoEx.MapEntryToProto(entry, _serializer),
        };

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private static async ValueTask<TResult> AwaitRpcGuardedAsync<TResult>(ValueTask<TResult> task)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (SquirixTransportException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }
        }

        private ValueTask<TResult> ExecuteCoreAsync<TState, TResult>(
            Func<BinaryCacheClient, TState, BinaryCallOptions, ValueTask<TResult>> action,
            TState state,
            CancellationToken cancellationToken) => _failover.ExecuteAsync(
            static (nodeId, execution, budget, ct) =>
            {
                var node = execution.Cache._clients.ForNode(nodeId);
                return node.Policy.ExecuteAsync(
                    static (policyState, token) => policyState.Action(
                        policyState.Client,
                        policyState.State,
                        new BinaryCallOptions(policyState.Budget.ForwardDeadlineUtc, token, policyState.CallerToken)),
                    (execution.Action, node.Client, execution.State, Budget: budget, execution.CallerToken),
                    ct,
                    budget);
            },
            (Cache: this, Action: action, State: state, CallerToken: cancellationToken),
            cancellationToken);

        private static class CacheOperationContract
        {
            internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, OperationIdRequiredException.StableDetail, StringComparison.Ordinal);

            internal static bool IsOperationIdReuseMismatchMessage(string? message) =>
                string.Equals(message, OperationIdReuseMismatchException.StableDetail, StringComparison.Ordinal);
        }

        private static class RemoteRpcErrorMapper
        {
            /// <summary>Applies remote RPC error mapping and always throws (never returns normally).</summary>
            /// <param name="ex">The transport exception from the remote cache pipeline.</param>
            /// <exception cref="OperationIdRequiredException">When the server rejected a missing operation id.</exception>
            /// <exception cref="OperationIdReuseMismatchException">When the server rejected an operation-id reuse mismatch.</exception>
            /// <exception cref="CommitOutcomeUnknownException">When the server reports an ambiguous durable commit outcome.</exception>
            /// <exception cref="SquirixTransportException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
            [DoesNotReturn]
            internal static void Map(SquirixTransportException ex)
            {
                ArgumentNullException.ThrowIfNull(ex);

                if (ex.StatusCode is CacheTransportStatusCode.InvalidArgument && CacheOperationContract.IsOperationIdRequiredMessage(ex.Status.Detail))
                    throw new OperationIdRequiredException(ex.Status.Detail, ex);

                if (ex.StatusCode is CacheTransportStatusCode.FailedPrecondition && CacheOperationContract.IsOperationIdReuseMismatchMessage(ex.Status.Detail))
                    throw new OperationIdReuseMismatchException(ex.Status.Detail, ex);

                var commitUnknown = CommitOutcomeUnknownClassifier.Map(ex);
                if (commitUnknown != null)
                    throw commitUnknown;

                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
    }
}
