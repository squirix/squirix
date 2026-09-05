using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Parameters for a leader-owned expired-read tombstone commit.</summary>
internal sealed class ReplicaExpirationRequest
{
    internal required string GroupId { get; init; }

    internal required string CacheName { get; init; }

    internal required string Key { get; init; }

    internal required DateTime UtcNow { get; init; }

    internal required Func<CancellationToken, ValueTask<ReplicaExpirationCandidate?>> ReadRaw { get; init; }

    internal required Func<ReplicaExpirationCandidate, string, PreparedReplicaMutation> PrepareTombstone { get; init; }

    internal required TimeSpan Timeout { get; init; }

    internal CancellationToken CancellationToken { get; init; }
}
