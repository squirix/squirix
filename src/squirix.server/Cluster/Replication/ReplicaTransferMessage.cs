using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>One message of a state transfer after the begin, as the receiver handles it.</summary>
[Immutable]
internal readonly struct ReplicaTransferMessage
{
    /// <summary>Gets the kind of the message.</summary>
    internal required ReplicaTransferStep Step { get; init; }

    /// <summary>Gets the replica group identifier.</summary>
    internal required string GroupId { get; init; }

    /// <summary>Gets the transfer the message belongs to.</summary>
    internal required string TransferId { get; init; }

    /// <summary>Gets the sequence number of the message; the begin was zero.</summary>
    internal required int Sequence { get; init; }

    /// <summary>Gets the offset of a snapshot chunk in the image.</summary>
    internal long Offset { get; init; }

    /// <summary>Gets the bytes of a snapshot chunk.</summary>
    internal ReadOnlyMemory<byte> Chunk { get; init; }

    /// <summary>Gets the entries of a batch.</summary>
    internal ReadOnlyMemory<ReplicaStateEntry> Entries { get; init; }

    /// <summary>Gets the number of entries the leader sent in total.</summary>
    internal ulong EntryCount { get; init; }

    /// <summary>Gets the CRC32C the leader computed over the entries.</summary>
    internal uint EntriesCrc { get; init; }
}
