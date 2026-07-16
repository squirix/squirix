using System;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Node.Observability;

/// <summary>Holds per-node inputs for idempotency observable gauges.</summary>
internal sealed class IdempotencyMetricRegistration
{
    public IdempotencyMetricRegistration(string nodeId, RpcMutationIdempotencyStore store)
    {
        NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        Store = store ?? throw new ArgumentNullException(nameof(store));
    }

    internal string NodeId { get; }

    internal RpcMutationIdempotencyStore Store { get; }
}
