using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Coordinates admitted adapter access at the routed owner.</summary>
[Immutable]
internal sealed class NodeDistributedCacheAccess : IDistributedCacheAccess
{
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly PhysicalCache<object?> _physical;
    private readonly JournalLoggingCacheDecorator<object?>? _durable;
    private readonly RpcMutationIdempotencyStore _outcomes;
    private readonly IRpcMutationIdempotencyCoordinator _idempotency;
    private readonly IBackpressureGate _gate;
    private readonly IBackpressureClientIdResolver _clients;

    internal NodeDistributedCacheAccess(
        ILogicalNamespacedCache<object?> local,
        PhysicalCache<object?> physical,
        JournalLoggingCacheDecorator<object?>? durable,
        RpcMutationIdempotencyStore outcomes,
        IRpcMutationIdempotencyCoordinator idempotency,
        IBackpressureGate gate,
        IBackpressureClientIdResolver clients)
    {
        _local = local;
        _physical = physical;
        _durable = durable;
        _outcomes = outcomes;
        _idempotency = idempotency;
        _gate = gate;
        _clients = clients;
    }

    /// <inheritdoc />
    public async Task<GetDistributedAsyncResponse> AccessAsync(string operationId, string cacheName, string key, Func<string, string, bool, string> fingerprintFactory, bool refresh, CancellationToken cancellationToken)
    {
        var operation = refresh ? CacheOperationNames.Touch : CacheOperationNames.Get;
        var (admission, lease) = await _gate.AcquireAsync(CacheOperationNames.CacheTransport, operation, _clients.Resolve(), cancellationToken).ConfigureAwait(false);
        if (!admission.IsAccepted)
            throw ServerOpContract.TooManyRequests(admission.RejectReason ?? "unknown").ToRpcException();

        using (lease)
        {
            return _local is ReplicatedCache replicated
                ? Response(await replicated.AccessDistributedAsync(operationId, cacheName, key, refresh, cancellationToken).ConfigureAwait(false))
                : await AccessSingleAsync(operationId, cacheName, key, fingerprintFactory, refresh, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Retains the immutable payload through response serialization and outcome replay.</summary>
    /// <param name="payload">Application bytes in an owned cache value or retained replica outcome, never a pooled buffer.</param>
    /// <returns>The response retaining the observed payload.</returns>
    private static GetDistributedAsyncResponse Response(ReadOnlyMemory<byte>? payload) =>
        payload is { } bytes ? new GetDistributedAsyncResponse { Found = true, Value = UnsafeByteOperations.UnsafeWrap(bytes) } : new GetDistributedAsyncResponse();

    private async Task<GetDistributedAsyncResponse> AccessSingleAsync(string operationId, string cacheName, string key, Func<string, string, bool, string> fingerprintFactory, bool refresh, CancellationToken cancellationToken)
    {
        if (_outcomes.HasRetainedOperation(operationId))
            return await ExecuteSingleAsync(operationId, cacheName, key, fingerprintFactory, refresh, cancellationToken).ConfigureAwait(false);

        var entry = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (_outcomes.HasRetainedOperation(operationId))
            return await ExecuteSingleAsync(operationId, cacheName, key, fingerprintFactory, refresh, cancellationToken).ConfigureAwait(false);

        if (entry == null)
            return new GetDistributedAsyncResponse();

        var payload = DistributedCacheAccess.ReadPayload(entry.Value, out var policy);
        var readOnlyResult = refresh ? default(ReadOnlyMemory<byte>?) : payload;
        return policy.SlidingTicks != null && !_physical.DistributedAccess.CanReadWithoutRenewal(entry)
            ? await ExecuteSingleAsync(operationId, cacheName, key, fingerprintFactory, refresh, cancellationToken).ConfigureAwait(false)
            : Response(readOnlyResult);
    }

    private Task<GetDistributedAsyncResponse> ExecuteSingleAsync(string operationId, string cacheName, string key, Func<string, string, bool, string> fingerprintFactory, bool refresh, CancellationToken cancellationToken) =>
        _idempotency.ExecuteAsync(
            operationId,
            static state => state.FingerprintFactory(state.CacheName, state.Key, state.Refresh),
            (Owner: this, OperationId: operationId, CacheName: cacheName, Key: key, FingerprintFactory: fingerprintFactory, Refresh: refresh),
            static async (state, token) =>
            {
                RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<ReadOnlyMemory<byte>?>(static payload => Response(payload));
                var payload = state.Owner._durable is { } durable
                    ? await durable.DistributedAccess.AccessDistributedAsync(state.OperationId, state.CacheName, state.Key, state.Refresh, token).ConfigureAwait(false)
                    : state.Owner._physical.DistributedAccess.AccessDistributed(new CacheKey(state.CacheName, state.Key), state.Refresh, token);
                return Response(payload);
            },
            cancellationToken);
}
