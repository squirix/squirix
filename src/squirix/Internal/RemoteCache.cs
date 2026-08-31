using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

[Immutable]
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
        return entry == null ? new CacheEntryResult<T>(false, null) : new CacheEntryResult<T>(true, entry);
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

        return _getOrAddFlights.RunAsync(key, new GetOrAddFlightState(this, key, valueFactory, options), static (state, ct) => ExecuteGetOrAddAsync(state, ct), cancellationToken);
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
        var response = await _rpc.ExecuteAsync(
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
        var response = await _rpc.ExecuteAsync(
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
        var entry = RemoteCacheRpc.ToEntry(value, options);
        OperationInputValidator.ValidateEntry(entry);
        var request = _rpc.ToSetEntryAsyncRequest(key, entry);
        request.OperationId = RpcOperationIdentity.New();

        _ = await _rpc.ExecuteAsync(
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
        var response = await _rpc.ExecuteAsync(
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
        var entry = RemoteCacheRpc.ToEntry(value, options);
        OperationInputValidator.ValidateEntry(entry);
        var request = _rpc.ToTryAddEntryAsyncRequest(key, entry);
        request.OperationId = RpcOperationIdentity.New();

        var response = await _rpc.ExecuteAsync(
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

        return response.Found
            ? new CacheValueResult<T>(true, await ProtoEx.FromCacheValueAsync<T>(response.Value, state.Cache._serializer).ConfigureAwait(false))
            : new CacheValueResult<T>(false, default);
    }

    private async Task<CacheEntry<T>?> GetEntryOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _rpc.ExecuteAsync(
            static (client, state, ct) =>
            {
                var responseAsync = client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = state.CacheName, Key = state.Key }, cancellationToken: ct).ResponseAsync;
                return new ValueTask<GetEntryAsyncResponse>(responseAsync);
            },
            (CacheName: _cacheName, Key: key),
            cancellationToken).ConfigureAwait(false);

        return response.Found ? await ProtoEx.MapProtoEntryToCacheEntryAsync<T>(response.Entry, _serializer).ConfigureAwait(false) : null;
    }

    [Immutable]
    private sealed record GetOrAddFlightState(RemoteCache<T> Cache, string Key, Func<string, CancellationToken, Task<T?>> ValueFactory, CacheEntryOptions? Options);

    /// <summary>Validates expiration arguments where a strictly positive duration is required (for example, touch operations).</summary>
    private static class ExpirationInputValidator
    {
        /// <summary>
        /// Ensures <paramref name="expiration" /> is greater than zero.
        /// </summary>
        /// <param name="expiration">The expiration to validate.</param>
        /// <param name="parameterName">The caller parameter name for exceptions.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="expiration" /> is zero or negative.</exception>
        internal static void ValidateRequiredPositive(TimeSpan expiration, string parameterName)
        {
            if (expiration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(parameterName, expiration, "expiration must be greater than zero.");
        }
    }

    /// <summary>Validates single-operation payloads such as cache entries and non-null factory delegates.</summary>
    private static class OperationInputValidator
    {
        /// <summary>Validates a cache entry reference.</summary>
        /// <param name="entry">The entry to validate.</param>
        internal static void ValidateEntry(CacheEntry<T>? entry) => ArgumentNullException.ThrowIfNull(entry);
    }

    [Immutable]
    private sealed class RemoteCacheRpc
    {
        private readonly string _cacheName;
        private readonly IClientPool _clients;
        private readonly EndpointFailover _failover;
        private readonly ISquirixSerializer _serializer;

        internal RemoteCacheRpc(string cacheName, EndpointFailover failover, IClientPool clients, ISquirixSerializer serializer)
        {
            _cacheName = cacheName;
            _failover = failover ?? throw new ArgumentNullException(nameof(failover));
            _clients = clients ?? throw new ArgumentNullException(nameof(clients));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        internal static CacheEntry<T> ToEntry(T? value, CacheEntryOptions? options)
        {
            if (options is { Expiration: not null, ExpiresAt: not null })
                throw new ArgumentException("Cache entry options cannot specify both Expiration and ExpiresAt; set at most one expiration mechanism.", nameof(options));

            return new CacheEntry<T>
            {
                Value = value,
                Expiration = options?.Expiration,
                ExpiresUtc = options?.ExpiresAt?.UtcDateTime,
            };
        }

        internal ValueTask<TResult> ExecuteAsync<TState, TResult>(
            Func<SquirixCacheService.SquirixCacheServiceClient, TState, CancellationToken, ValueTask<TResult>> action,
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

        private static async ValueTask<TResult> AwaitRpcGuardedAsync<TResult>(ValueTask<TResult> task)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                RemoteRpcErrorMapper.Map(ex);
                throw;
            }
        }

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

        private static class CacheOperationContract
        {
            private const string InsertVersionMustExceedCurrentPrefix = "Version must be greater than current (current=";

            /// <summary>
            /// Determines whether <paramref name="detail" /> matches the stable increment counter type-mismatch contract (FailedPrecondition),
            /// distinct from CAS <c language="csharp">Version mismatch</c> and routing <c language="csharp">StaleOwner</c> texts.
            /// </summary>
            /// <param name="detail">The gRPC status detail string.</param>
            /// <returns><see langword="true" /> when <paramref name="detail" /> identifies a counter-increment type mismatch.</returns>
            internal static bool IsCounterIncrementTypeMismatchRpcDetail(string? detail) => !string.IsNullOrWhiteSpace(detail) &&
                                                                                            detail.Contains("Type mismatch", StringComparison.OrdinalIgnoreCase) && detail.Contains(
                                                                                                "expected",
                                                                                                StringComparison.OrdinalIgnoreCase);

            /// <summary>
            /// Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.
            /// </summary>
            /// <param name="message">An exception or RPC status detail string.</param>
            /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
            internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                             message.StartsWith(
                                                                                                 InsertVersionMustExceedCurrentPrefix,
                                                                                                 StringComparison.Ordinal) && message.Contains(
                                                                                                 ", provided=",
                                                                                                 StringComparison.Ordinal);

            internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, OperationIdRequiredException.StableDetail, StringComparison.Ordinal);

            internal static bool IsOperationIdReuseMismatchMessage(string? message) =>
                string.Equals(message, OperationIdReuseMismatchException.StableDetail, StringComparison.Ordinal);
        }

        /// <summary>Deterministic classification helpers shared by transport mappers; does not perform HTTP or gRPC result mapping.</summary>
        private static class CacheOperationContractClassifier
        {
            /// <summary>
            /// Stable contract classification for cache-operation transport faults that must stay aligned across
            /// gRPC adapters, remote cluster helpers, and <c language="csharp">DomainTransportErrorMapper</c>.
            /// </summary>
            private enum CacheOperationFailedPreconditionKind
            {
                /// <summary>No recognized stable contract for the given detail string.</summary>
                None = 0,

                /// <summary>Counter-increment type mismatch (FailedPrecondition detail).</summary>
                CounterIncrementTypeMismatch = 1,

                /// <summary>Explicit insert version is not greater than the stored version (FailedPrecondition detail).</summary>
                InsertVersionMustExceedCurrent = 2,

                /// <summary>Operation id was reused with a different mutation fingerprint (FailedPrecondition detail).</summary>
                OperationIdReuseMismatch = 3,
            }

            internal static bool IsOperationIdReuseMismatchDetail(string? detail) =>
                ClassifyFailedPreconditionDetail(detail) is CacheOperationFailedPreconditionKind.OperationIdReuseMismatch;

            /// <summary>
            /// Classifies <see cref="StatusCode.FailedPrecondition" /> status detail strings that map to a stable
            /// <see cref="InvalidOperationException" /> in the logical cache pipeline.
            /// </summary>
            /// <param name="detail">The gRPC status detail string.</param>
            /// <returns>The classified contract kind; <see cref="CacheOperationFailedPreconditionKind.None" /> when no stable contract matches.</returns>
            /// <remarks>
            /// Classification order matches the domain transport error mapper historical behavior:
            /// counter-increment type mismatch is evaluated before insert-version precondition text.
            /// </remarks>
            private static CacheOperationFailedPreconditionKind ClassifyFailedPreconditionDetail(string? detail)
            {
                if (CacheOperationContract.IsCounterIncrementTypeMismatchRpcDetail(detail))
                    return CacheOperationFailedPreconditionKind.CounterIncrementTypeMismatch;

                if (CacheOperationContract.IsInsertVersionMustExceedCurrentMessage(detail))
                    return CacheOperationFailedPreconditionKind.InsertVersionMustExceedCurrent;

                if (CacheOperationContract.IsOperationIdReuseMismatchMessage(detail))
                    return CacheOperationFailedPreconditionKind.OperationIdReuseMismatch;

                return CacheOperationFailedPreconditionKind.None;
            }
        }

        private static class RemoteRpcErrorMapper
        {
            /// <summary>Applies remote RPC error mapping and always throws (never returns normally).</summary>
            /// <param name="ex">The gRPC transport exception from the remote cache pipeline.</param>
            /// <exception cref="OperationIdRequiredException">When the server rejected a missing operation id.</exception>
            /// <exception cref="OperationIdReuseMismatchException">When the server rejected an operation-id reuse mismatch.</exception>
            /// <exception cref="RpcException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
            [DoesNotReturn]
            internal static void Map(RpcException ex)
            {
                ArgumentNullException.ThrowIfNull(ex);

                if (ex.StatusCode is StatusCode.InvalidArgument && CacheOperationContract.IsOperationIdRequiredMessage(ex.Status.Detail))
                    throw new OperationIdRequiredException(ex.Status.Detail, ex);

                if (ex.StatusCode is StatusCode.FailedPrecondition && CacheOperationContractClassifier.IsOperationIdReuseMismatchDetail(ex.Status.Detail))
                    throw new OperationIdReuseMismatchException(ex.Status.Detail, ex);

                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
    }
}
