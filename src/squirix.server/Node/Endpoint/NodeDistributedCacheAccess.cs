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

namespace Squirix.Server.Node.Endpoint;

/// <summary>Coordinates admitted adapter access at the routed owner.</summary>
[Immutable]
internal sealed class NodeDistributedCacheAccess : IDistributedCacheAccess
{
    private readonly ILogicalNamespacedCache<StoredValue> _local;
    private readonly PhysicalCache<StoredValue> _physical;
    private readonly JournalLoggingCacheDecorator<StoredValue>? _durable;
    private readonly IBackpressureGate _gate;
    private readonly IBackpressureClientIdResolver _clients;

    internal NodeDistributedCacheAccess(
        ILogicalNamespacedCache<StoredValue> local,
        PhysicalCache<StoredValue> physical,
        JournalLoggingCacheDecorator<StoredValue>? durable,
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
    public async ValueTask<ReadOnlyMemory<byte>?> AccessAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken)
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
            return await replicated.AccessDistributedAsync(operationId, cacheName, key, refresh, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<ReadOnlyMemory<byte>?> AccessSingleAsync(string cacheName, string key, bool refresh, CancellationToken cancellationToken)
    {
        var entry = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        if (entry == null)
            return null;

        var payload = DistributedCacheAccess.ReadPayload(entry.Value, out var policy);
        var result = refresh ? default(ReadOnlyMemory<byte>?) : payload;
        if (policy.SlidingTicks != null && !_physical.CanReadWithoutRenewal(entry))
        {
            result = _durable is { } durable
                ? await durable.DistributedAccess.AccessDistributedAsync(string.Empty, cacheName, key, refresh, cancellationToken).ConfigureAwait(false)
                : _physical.AccessDistributed(new CacheKey(cacheName, key), refresh, cancellationToken);
        }

        return result;
    }
}
