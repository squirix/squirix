using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Rebuilds the prepared form of uncommitted leader log entries recovered after a restart.</summary>
/// <remarks>
/// A durable log record carries the mutation but not the outcome its prepare observed. A recovered entry is applied in
/// log order after every predecessor, so the memory it is applied to is the memory its prepare read: the outcome is read
/// again right before the apply, from the same state.
/// </remarks>
internal interface IReplicaTailRebuilder
{
    /// <summary>Rebuilds the prepared mutation carried by a recovered log entry, without its outcome.</summary>
    /// <param name="entry">Recovered leader log entry.</param>
    /// <returns>The prepared mutation with an empty outcome payload.</returns>
    /// <exception cref="System.IO.InvalidDataException">The entry payload is not a canonical replica log record.</exception>
    PreparedReplicaMutation Rebuild(FollowerLogEntry entry);

    /// <summary>Reads the outcome of a recovered entry from live memory, right before its apply.</summary>
    /// <param name="entry">Recovered entry about to be applied.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The canonical outcome payload the entry's prepare would have observed.</returns>
    ValueTask<ReadOnlyMemory<byte>> ReadOutcomeAsync(PreparedReplicaMutation entry, CancellationToken cancellationToken);
}
