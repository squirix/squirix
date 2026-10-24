using System;
using System.Collections.Generic;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Checks and cleanup shared by the commits of a dense run of prepared mutations.</summary>
internal static class ReplicaCommitRuns
{
    /// <summary>Checks that a run is non-empty, within the acknowledgement buffer of a slot, and dense in one term.</summary>
    /// <param name="run">The run.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="run" /> is empty or longer than <see cref="ReplicaCommitQuorum.MaxBufferedAcks" />.</exception>
    /// <exception cref="InvalidOperationException">The log indexes of <paramref name="run" /> are not consecutive in one term.</exception>
    internal static void Validate(IReadOnlyList<PreparedReplicaMutation> run)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentOutOfRangeException.ThrowIfLessThan(run.Count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(run.Count, ReplicaCommitQuorum.MaxBufferedAcks);

        var first = run[0];
        for (var i = 1; i < run.Count; i++)
        {
            var mutation = run[i];
            if (mutation.Term != first.Term || mutation.LogIndex != first.LogIndex + ulong.CreateChecked(i))
                throw new InvalidOperationException("The mutations of a run must have consecutive log indexes in one term.");
        }
    }

    /// <summary>Releases the unresolved idempotency pins of the first entries of a run.</summary>
    /// <param name="idempotency">The group idempotency state.</param>
    /// <param name="run">The run.</param>
    /// <param name="count">How many entries from the start hold a pin.</param>
    internal static void ReleasePins(GroupIdempotencyState idempotency, IReadOnlyList<PreparedReplicaMutation> run, int count)
    {
        for (var i = 0; i < count; i++)
            _ = idempotency.TryReleaseUnresolved(run[i].OperationScope, run[i].OperationId, run[i].LogIndex, run[i].Term);
    }
}
