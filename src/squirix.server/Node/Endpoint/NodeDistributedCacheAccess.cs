using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Coordinates admitted adapter access at the routed owner.</summary>
[Immutable]
internal sealed class NodeDistributedCacheAccess : IDistributedCacheAccess
{
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly PhysicalCache<object?> _physical;
    private readonly JournalLoggingCacheDecorator<object?>? _durable;
    private readonly IBackpressureGate _gate;
    private readonly IBackpressureClientIdResolver _clients;

    internal NodeDistributedCacheAccess(
        ILogicalNamespacedCache<object?> local,
        PhysicalCache<object?> physical,
        JournalLoggingCacheDecorator<object?>? durable,
        IBackpressureGate gate,
        IBackpressureClientIdResolver clients)
    {
        _local = local;
        _physical = physical;
        _durable = durable;
        _gate = gate;
        _clients = clients;
    }

    /// <inheritdoc />
    public async Task<GetDistributedAsyncResponse> AccessAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken)
    {
        var operation = refresh ? CacheOperationNames.Touch : CacheOperationNames.Get;
        var (admission, lease) = await InboundAdmissionScope.AcquireIfNeededAsync(_gate, CacheOperationNames.CacheTransport, operation, _clients, cancellationToken).ConfigureAwait(false);
        if (!admission.IsAccepted)
            throw ServerOpContract.TooManyRequests(admission.RejectReason ?? "unknown").ToServerCallException();

        using (lease)
        {
            if (_local is not ReplicatedCache replicated)
                return await AccessSingleAsync(cacheName, key, refresh, cancellationToken).ConfigureAwait(false);

            if (refresh)
                operationId = RpcMutationContracts.RequireOperationId(operationId);
            return Response(await replicated.AccessDistributedAsync(operationId, cacheName, key, refresh, cancellationToken).ConfigureAwait(false));
        }
    }

    /// <summary>Retains the immutable payload only through response serialization.</summary>
    /// <param name="payload">Application bytes in an owned cache value, never a pooled buffer.</param>
    /// <returns>The response retaining the observed payload.</returns>
    private static GetDistributedAsyncResponse Response(ReadOnlyMemory<byte>? payload) =>
        payload is { } bytes ? new GetDistributedAsyncResponse { Found = true, Value = bytes } : new GetDistributedAsyncResponse();

    private async Task<GetDistributedAsyncResponse> AccessSingleAsync(string cacheName, string key, bool refresh, CancellationToken cancellationToken)
    {
        var entry = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (entry == null)
            return new GetDistributedAsyncResponse();

        var payload = DistributedCacheAccess.ReadPayload(entry.Value, out var policy);
        if (policy.SlidingTicks == null || _physical.DistributedAccess.CanReadWithoutRenewal(entry))
            return Response(refresh ? null : payload);

        var renewed = _durable is { } durable
            ? await durable.DistributedAccess.AccessDistributedAsync(string.Empty, cacheName, key, refresh, cancellationToken).ConfigureAwait(false)
            : _physical.DistributedAccess.AccessDistributed(new CacheKey(cacheName, key), refresh, cancellationToken);
        return Response(renewed);
    }
}
