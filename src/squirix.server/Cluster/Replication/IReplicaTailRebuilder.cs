using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Rebuilds the prepared form of uncommitted leader log entries recovered after a restart.</summary>
/// <remarks>
/// A durable log record carries the outcome and the effect its prepare decided, so a recovered entry is rebuilt from the record
/// alone: nothing is read from memory, and the outcome it reports equals the one the original prepare gave the client.
/// </remarks>
internal interface IReplicaTailRebuilder
{
    /// <summary>Rebuilds the prepared mutation carried by a recovered log entry, with the outcome its record holds.</summary>
    /// <param name="entry">Recovered leader log entry.</param>
    /// <returns>The prepared mutation, whose outcome payload is the one of the record.</returns>
    /// <exception cref="System.IO.InvalidDataException">The entry payload is not a canonical replica log record, or the record is inconsistent.</exception>
    PreparedReplicaMutation Rebuild(FollowerLogEntry entry);
}
