using System;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit;

/// <summary>Test helpers for building follower log entry batches from literals.</summary>
internal static class FollowerLogEntryExtensions
{
    /// <param name="batch">The batch type the helpers build.</param>
    extension(ReadOnlyMemory<FollowerLogEntry> batch)
    {
        /// <summary>Copies <paramref name="entries" /> into a batch that outlives the call.</summary>
        /// <param name="entries">The entries in log order.</param>
        /// <returns>A batch holding the entries.</returns>
        internal static ReadOnlyMemory<FollowerLogEntry> Of(params ReadOnlySpan<FollowerLogEntry> entries)
        {
            var copy = new FollowerLogEntry[entries.Length];
            entries.CopyTo(copy);
            return copy;
        }
    }
}
