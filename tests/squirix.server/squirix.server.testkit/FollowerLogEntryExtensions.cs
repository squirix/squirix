using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit;

/// <summary>Test helpers for building follower log entry batches from literals.</summary>
internal static class FollowerLogEntryExtensions
{
    extension(ReadOnlyMemory<FollowerLogEntry>)
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

    extension(FollowerLogEntry)
    {
        /// <summary>Encodes <paramref name="record" /> into the log entry that carries it at its own position.</summary>
        /// <param name="record">The record to encode.</param>
        /// <returns>The entry holding the canonical bytes of the record.</returns>
        internal static FollowerLogEntry Of(in ReplicaLogRecord record) => new(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));
    }
}
