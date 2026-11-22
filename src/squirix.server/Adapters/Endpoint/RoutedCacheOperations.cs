using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint;

[Immutable]
internal sealed class RoutedCacheOperations<T>
{
    internal RoutedCacheOperations(
        IInboundEndpointCacheOperations<T> cacheOperations,
        OwnerRouter router,
        OwnerRpcForwarder forwarder,
        bool replicated,
        TimeProvider timeProvider,
        IDistributedCacheAccess? distributedAccess = null,
        IJournalCoordinator? journal = null)
    {
        ArgumentNullException.ThrowIfNull(cacheOperations);
        ArgumentNullException.ThrowIfNull(router);
        ArgumentNullException.ThrowIfNull(forwarder);
        ArgumentNullException.ThrowIfNull(timeProvider);
        Reads = new ReadOperations(cacheOperations, router, forwarder, timeProvider, distributedAccess);
        Mutations = new MutationOperations(cacheOperations, router, forwarder, replicated, journal);
    }

    internal ReadOperations Reads { get; }

    internal MutationOperations Mutations { get; }

    [Immutable]
    internal sealed class ReadOperations
    {
        private readonly IInboundEndpointCacheOperations<T> _cacheOperations;
        private readonly IDistributedCacheAccess? _distributedAccess;
        private readonly OwnerRpcForwarder _forwarder;
        private readonly OwnerRouter _router;
        private readonly TimeProvider _timeProvider;
        private readonly IRoutedKeyContext? _keyValidation;

        internal ReadOperations(IInboundEndpointCacheOperations<T> cacheOperations, OwnerRouter router, OwnerRpcForwarder forwarder, TimeProvider timeProvider, IDistributedCacheAccess? distributedAccess)
        {
            _cacheOperations = cacheOperations;
            _router = router;
            _forwarder = forwarder;
            _timeProvider = timeProvider;
            _distributedAccess = distributedAccess;
            _keyValidation = router.KeyContext;
        }

        internal Task<GetDistributedAsyncResponse> GetDistributedAsync(GetDistributedAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.GetDistributedAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter.AccessDistributedLocalAsync(string.Empty, s.Request.CacheName, s.Request.Key, false, ct),
            cancellationToken);

        internal Task<GetEntryAsyncResponse> GetEntryAsync(GetEntryAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.GetEntryAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter.GetEntryLocalAsync(s.Request, ct),
            cancellationToken);

        internal Task<GetExpirationAsyncResponse> GetExpirationAsync(GetExpirationAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.GetExpirationAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter.GetExpirationLocalAsync(s.Request, ct),
            cancellationToken);

        internal Task<GetValueAsyncResponse> GetValueAsync(GetValueAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.GetValueAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter.GetValueLocalAsync(s.Request, ct),
            cancellationToken);

        internal Task<RefreshDistributedAsyncResponse> RefreshDistributedAsync(RefreshDistributedAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.RefreshDistributedAsync(owner, s.Request, ct),
            static async (s, ct) =>
            {
                _ = await s.Adapter.AccessDistributedLocalAsync(s.Request.OperationId, s.Request.CacheName, s.Request.Key, true, ct).ConfigureAwait(false);
                return new RefreshDistributedAsyncResponse();
            },
            cancellationToken);

        private Task<GetDistributedAsyncResponse> AccessDistributedLocalAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken)
        {
            var cache = SquirixServiceAdapterValidation.RequireCacheName(cacheName);
            SquirixServiceAdapterValidation.RequireValidCacheKey(key, _keyValidation);
            var access = _distributedAccess ??
                         ThrowHelper.Throw<IDistributedCacheAccess>(new ServerCallException(FrameStatus.Unimplemented, "Distributed cache access is unavailable."));
            return access.AccessAsync(operationId, cache, key, refresh, cancellationToken);
        }

        private ICacheApi<T> ApiForRequest(string cacheName) => _cacheOperations.ForCache(SquirixServiceAdapterValidation.RequireCacheName(cacheName));

        private async Task<GetEntryAsyncResponse> GetEntryLocalAsync(GetEntryAsyncRequest request, CancellationToken cancellationToken)
        {
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);
            var entry = await ApiForRequest(request.CacheName).GetEntryAsync(request.Key, cancellationToken).ConfigureAwait(false);
            if (entry == null)
                return new GetEntryAsyncResponse { Found = false };

            var response = new GetEntryAsyncResponse { Found = true, Entry = entry.MapToWire(false) };
            if (entry.ExpiresUtc is { } expiresUtc)
                response.Remaining = RemainingUntil(expiresUtc);

            return response;
        }

        private async Task<GetExpirationAsyncResponse> GetExpirationLocalAsync(GetExpirationAsyncRequest request, CancellationToken cancellationToken)
        {
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);
            var read = await ApiForRequest(request.CacheName).GetValueWithDeadlineAsync(request.Key, cancellationToken).ConfigureAwait(false);
            if (!read.Found)
                return new GetExpirationAsyncResponse { Found = false };

            var response = new GetExpirationAsyncResponse { Found = true };
            if (read.ExpiresUtc is not { } expiresUtc)
                return response;

            response.HasExpiration = true;
            response.Remaining = RemainingUntil(expiresUtc);
            return response;
        }

        private async Task<GetValueAsyncResponse> GetValueLocalAsync(GetValueAsyncRequest request, CancellationToken cancellationToken)
        {
            SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);
            var result = await ApiForRequest(request.CacheName).GetValueAsync(request.Key, cancellationToken).ConfigureAwait(false);
            var response = new GetValueAsyncResponse { Found = result.Found };
            if (result.Found)
                response.Value = ServerWireMapping.CacheValueToWire(result.RawValue);

            return response;
        }

        /// <summary>Returns the time left before <paramref name="expiresUtc" /> on the server clock, never negative.</summary>
        /// <param name="expiresUtc">The entry deadline.</param>
        /// <returns>The remaining time.</returns>
        private TimeSpan RemainingUntil(DateTime expiresUtc)
        {
            var remaining = expiresUtc - _timeProvider.GetUtcNow().UtcDateTime;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }

    [Immutable]
    internal sealed class MutationOperations
    {
        private readonly OwnerRpcForwarder _forwarder;
        private readonly MutationHandlers _handlers;
        private readonly OwnerRouter _router;

        internal MutationOperations(IInboundEndpointCacheOperations<T> cacheOperations, OwnerRouter router, OwnerRpcForwarder forwarder, bool replicated, IJournalCoordinator? journal)
        {
            _router = router;
            _forwarder = forwarder;
            _handlers = new MutationHandlers(cacheOperations, replicated, journal, router.KeyContext);
        }

        internal Task<GetOrAddAsyncResponse> GetOrAddAsync(GetOrAddAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.GetOrAddAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.GetOrAddAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<RemoveAsyncResponse> RemoveAsync(RemoveAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.RemoveAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.RemoveAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<RemoveExpirationAsyncResponse> RemoveExpirationAsync(RemoveExpirationAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.RemoveExpirationAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.RemoveExpirationAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<SetAsyncResponse> SetDistributedAsync(SetDistributedAsyncRequest request, CancellationToken cancellationToken) =>
            _router.ExecuteAsync(
                request.CacheName,
                request.Key,
                (Adapter: this, Request: request, OriginalValue: request.Value),
                static (s, owner, ct) =>
                {
                    // The first forward takes ownership. A reroute reuses the same owned bytes.
                    if (!s.OriginalValue.IsEmpty && s.Request.Value.Equals(s.OriginalValue))
                        s.Request.Value = s.OriginalValue.ToArray();

                    return s.Adapter._forwarder.SetDistributedAsync(owner, s.Request, ct);
                },
                static (s, ct) => s.Adapter._handlers.SetDistributedAsyncCoreAsync(s.Request, s.Request.Value, ct),
                cancellationToken);

        internal Task<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.SetEntryAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.SetEntryAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<TouchAsyncResponse> TouchAsync(TouchAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.TouchAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.TouchAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(TryAddEntryAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.AddEntryIfAbsentAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.AddEntryAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        internal Task<UpdateAsyncResponse> UpdateAsync(UpdateAsyncRequest request, CancellationToken cancellationToken) => _router.ExecuteAsync(
            request.CacheName,
            request.Key,
            (Adapter: this, Request: request),
            static (s, owner, ct) => s.Adapter._forwarder.UpdateAsync(owner, s.Request, ct),
            static (s, ct) => s.Adapter._handlers.UpdateAsyncCoreAsync(s.Request, ct),
            cancellationToken);

        [Immutable]
        private sealed class MutationHandlers
        {
            private readonly IInboundEndpointCacheOperations<T> _cacheOperations;

            private readonly bool _replicated;
            private readonly IJournalCoordinator? _journal;
            private readonly IRoutedKeyContext? _keyValidation;

            internal MutationHandlers(IInboundEndpointCacheOperations<T> cacheOperations, bool replicated, IJournalCoordinator? journal, IRoutedKeyContext? keyValidation)
            {
                _cacheOperations = cacheOperations;
                _replicated = replicated;
                _journal = journal;
                _keyValidation = keyValidation;
            }

            internal async Task<TryAddAsyncResponse> AddEntryAsyncCoreAsync(TryAddEntryAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                var added = await _cacheOperations.ForCache(cacheName).TryAddEntryAsync(
                    operationId,
                    request.Key,
                    request.Entry.MapFromWire<T>(),
                    cancellationToken).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            }

            internal async Task<GetOrAddAsyncResponse> GetOrAddAsyncCoreAsync(GetOrAddAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);
                var api = _cacheOperations.ForCache(cacheName);

                // The add runs first, with the operation id: a retry whose RPC record is gone replays the recorded outcome of the add instead
                // of reading the value its first attempt added and answering that nothing was added. An add over a present key without a
                // recorded outcome is refused before it reaches the journal or the replica group.
                var entry = request.Entry.MapFromWire<T>();

                // The response of an add that took effect carries no value and is known before the add is applied.
                var (added, existing) = await api.AddOrGetAsync(operationId, request.Key, entry, cancellationToken)
                                                 .ConfigureAwait(false);
                return added
                    ? AddedResponse()
                    : existing.Found switch
                    {
                        false => new GetOrAddAsyncResponse { Added = false, Found = false },
                        _ => new GetOrAddAsyncResponse { Added = false, Found = true, Value = ServerWireMapping.CacheValueToWire(existing.RawValue) },
                    };
            }

            internal async Task<RemoveAsyncResponse> RemoveAsyncCoreAsync(RemoveAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                var result = await _cacheOperations.ForCache(cacheName).RemoveAsync(operationId, request.Key, cancellationToken)
                                                   .ConfigureAwait(false);
                return RemoveResponse(result);
            }

            internal async Task<RemoveExpirationAsyncResponse> RemoveExpirationAsyncCoreAsync(RemoveExpirationAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                var found = await _cacheOperations.ForCache(cacheName)
                                                  .RemoveExpirationAsync(operationId, request.Key, cancellationToken)
                                                  .ConfigureAwait(false);
                return new RemoveExpirationAsyncResponse { Found = found };
            }

            internal async Task<SetAsyncResponse> SetDistributedAsyncCoreAsync(SetDistributedAsyncRequest request, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);
                var policy = new DistributedCachePolicy(request.HasExpiresAtUnixMs ? request.ExpiresAtUnixMs : null, request.HasSlidingTicks ? request.SlidingTicks : null);
                long? ttl = request.HasTtlTicks ? request.TtlTicks : null;
                if (!policy.IsValid() || ttl is <= 0)
                    throw new ServerCallException(FrameStatus.InvalidArgument, "The distributed cache expiration policy is invalid.");

                var write = new DistributedCacheWrite(in policy, payload.Span, ttl);
                if (write is not T value)
                    throw new ServerCallException(FrameStatus.Internal, "The node cannot store distributed cache values.");

                await _cacheOperations.ForCache(cacheName).SetEntryAsync(
                    operationId,
                    request.Key,
                    new NodeCacheEntry<T>(value, expiresUtc: write.DeadlineForSizing),
                    cancellationToken).ConfigureAwait(false);
                return new SetAsyncResponse();
            }

            internal async Task<SetAsyncResponse> SetEntryAsyncCoreAsync(SetEntryAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                await _cacheOperations.ForCache(cacheName).SetEntryAsync(
                    operationId,
                    request.Key,
                    request.Entry.MapFromWire<T>(),
                    cancellationToken).ConfigureAwait(false);
                return new SetAsyncResponse();
            }

            internal async Task<TouchAsyncResponse> TouchAsyncCoreAsync(TouchAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                var found = await _cacheOperations.ForCache(cacheName).TouchAsync(
                    operationId,
                    request.Key,
                    request.Expiration ?? MissingExpiration(),
                    cancellationToken).ConfigureAwait(false);
                return new TouchAsyncResponse { Found = found };
            }

            internal async Task<UpdateAsyncResponse> UpdateAsyncCoreAsync(UpdateAsyncRequest request, CancellationToken cancellationToken)
            {
                var operationId = _replicated ? RpcMutationContracts.RequireOperationId(request.OperationId) : string.Empty;
                if (_journal != null)
                    await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

                var cacheName = SquirixServiceAdapterValidation.RequireCacheName(request.CacheName);
                SquirixServiceAdapterValidation.RequireValidCacheKey(request.Key, _keyValidation);

                var updated = await _cacheOperations.ForCache(cacheName).UpdateAsync(
                    operationId,
                    request.Key,
                    request.Entry.MapFromWire<T>().RawValue,
                    cancellationToken).ConfigureAwait(false);
                return new UpdateAsyncResponse { Updated = updated };
            }

            private static GetOrAddAsyncResponse AddedResponse() => new() { Added = true, Found = true };

            private static RemoveAsyncResponse RemoveResponse(bool result) => new() { Removed = result };

            private static TimeSpan MissingExpiration() => throw new ServerCallException(FrameStatus.InvalidArgument, "Missing expiration.");
        }
    }
}
