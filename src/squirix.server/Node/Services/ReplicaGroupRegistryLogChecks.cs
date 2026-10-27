using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Reads of the owned group log that decide how a failed replicated commit is reported.</summary>
internal static class ReplicaGroupRegistryLogChecks
{
    extension(ReplicaGroupRegistry registry)
    {
        /// <summary>Determines whether the log of a group still holds a prepared entry, with the term it was prepared in.</summary>
        /// <param name="groupId">The group whose log is read.</param>
        /// <param name="mutation">The prepared mutation whose entry is looked up.</param>
        /// <returns><see langword="true" /> when the log holds the entry; <see langword="false" /> when it does not, or when the log cannot tell.</returns>
        internal async ValueTask<bool> HoldsEntryAsync(string groupId, PreparedReplicaMutation mutation)
        {
            if (!registry.TryGetLog(groupId, out var log))
                return false;

            try
            {
                var status = await log.GetStatusAsync(CancellationToken.None).ConfigureAwait(false);
                return status.LastLogIndex >= mutation.LogIndex && await HoldsTermAtAsync(log, mutation).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // A closed log cannot tell; the original failure is reported as it is.
                return false;
            }
        }
    }

    /// <summary>Tells whether the log holds the term of a prepared entry at its index, which a moment ago was within the log.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="mutation">The prepared mutation.</param>
    /// <returns><see langword="true" /> when the term at the index of the entry is the term it was prepared in.</returns>
    /// <exception cref="InvalidDataException">The log retains no term at an index it still holds: its state is damaged.</exception>
    /// <remarks>
    /// The status and the term are two reads of the log. A truncation between them removes the index; that log no longer holds the entry,
    /// which a second status read proves. Any other missing term is reported as it is.
    /// </remarks>
    private static async ValueTask<bool> HoldsTermAtAsync(IFollowerLog log, PreparedReplicaMutation mutation)
    {
        try
        {
            return await log.GetTermAtAsync(mutation.LogIndex, CancellationToken.None).ConfigureAwait(false) == mutation.Term;
        }
        catch (InvalidDataException)
        {
            if ((await log.GetStatusAsync(CancellationToken.None).ConfigureAwait(false)).LastLogIndex < mutation.LogIndex)
                return false;
            throw;
        }
    }
}
