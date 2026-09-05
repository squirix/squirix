using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Leader-authoritative inputs for one snapshot catch-up session.</summary>
[Immutable]
internal sealed class ReplicaSnapshotCatchUpRequest
{
    /// <summary>Gets the independent state finalizer and checksum reader.</summary>
    internal required Func<CancellationToken, ValueTask<uint>> FinalizeStateAsync { get; init; }

    /// <summary>Gets the expected final durable and state-machine progress.</summary>
    internal required ReplicaProgress Expected { get; init; }

    /// <summary>Gets the current leader node identity.</summary>
    internal required string LeaderNodeId { get; init; }

    /// <summary>Gets the current leader term.</summary>
    internal required ulong LeaderTerm { get; init; }

    /// <summary>Gets the verified snapshot transfer.</summary>
    internal required ReplicaSnapshotTransfer Snapshot { get; init; }

    /// <summary>Gets retained entries above the snapshot baseline.</summary>
    internal required IReadOnlyList<FollowerLogEntry> TailEntries { get; init; }
}
