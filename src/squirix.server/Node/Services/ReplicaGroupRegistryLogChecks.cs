using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

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
                return status.LastLogIndex >= mutation.LogIndex && await log.GetTermAtAsync(mutation.LogIndex, CancellationToken.None).ConfigureAwait(false) == mutation.Term;
            }
            catch (ObjectDisposedException)
            {
                // A closed log cannot tell; the original failure is reported as it is.
                return false;
            }
        }
    }
}
