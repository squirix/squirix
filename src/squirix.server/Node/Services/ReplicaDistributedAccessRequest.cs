using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>A serialized adapter access whose response bytes belong only to its active caller.</summary>
/// <remarks>
/// The leader captures the payload from the same entry and clock as the renewal decision. The completion
/// signal publishes it only after commit and apply. Durable outcomes contain no reference to these bytes.
/// Refresh uses the client identity; Get receives a new server identity for every transport attempt.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaDistributedAccessRequest : ReplicaCommitRequest
{
    private readonly bool _refresh;

    internal ReplicaDistributedAccessRequest(string operationId, string cacheName, string key, bool refresh)
        : base(cacheName, operationId, key)
    {
        _refresh = refresh;
        ReplayBeforePrepare = refresh;
    }

    /// <summary>Gets the immutable bytes captured at prepare, read by the caller after completion.</summary>
    internal ReadOnlyMemory<byte>? Payload { get; private set; }

    /// <summary>Releases the response reference once the caller has copied it or the commit failed.</summary>
    internal void ReleasePayload() => Payload = null;

    /// <inheritdoc />
    internal override GroupIdempotencyLookup LookupRetained(IFollowerLog? log, out GroupIdempotencyRecord record)
    {
        record = default;
        return log == null || log.Idempotency.Lookup(Scope, OperationId, [], out _) == GroupIdempotencyLookup.Miss
            ? GroupIdempotencyLookup.Miss
            : log.Idempotency.Lookup(Scope, OperationId, ReplicaOperationFingerprints.DistributedAccess(OperationId, Scope, Key, _refresh), out record);
    }

    /// <inheritdoc />
    internal override async ValueTask<PreparedReplicaMutation> PrepareAsync(ReplicaMutationFactory factory, ulong index, CancellationToken cancellationToken)
    {
        var prepared = await factory.PrepareDistributedAccessAsync(OperationId, Scope, Key, _refresh, index, cancellationToken).ConfigureAwait(false);
        Payload = prepared.Payload;
        return prepared.Mutation;
    }
}
