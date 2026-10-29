using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Settings of the receiver of a state transfer.</summary>
[Immutable]
internal sealed class ReplicaTransferOptions
{
    /// <summary>
    /// The payload bytes one transfer message may carry: 7 MiB, which leaves one MiB of the 8 MiB gRPC message limit for the envelope and the framing.
    /// A snapshot chunk counts its bytes; an entry batch counts, per entry, the UTF-8 bytes of the cache name, the key and the payload plus <see cref="EntryFramingBytes" />.
    /// </summary>
    internal const int MessageBudgetBytes = EntryLimits.GrpcMaxSendMessageSizeBytes - (1024 * 1024);

    /// <summary>The bytes the wire adds to one entry of a batch beyond its cache name, key and payload: field tags, lengths and the deadline.</summary>
    internal const int EntryFramingBytes = 32;

    /// <summary>Gets the longest time a transfer may stay silent before the group is released; 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is not positive.</exception>
    internal TimeSpan IdleTimeout
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            field = value;
        }
    } = TimeSpan.FromSeconds(30);

    /// <summary>Gets the longest time a follower may keep a transfer step busy because its journal is at the high-water mark before the leader gives the transfer up; 5 minutes unless set.</summary>
    /// <remarks>The time restarts with every accepted reply, so a transfer that makes progress between pauses is never ended by it.</remarks>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan JournalStallBudget
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            field = value;
        }
    } = TimeSpan.FromMinutes(5);

    /// <summary>Gets the largest published snapshot image the receiver buffers.</summary>
    internal int MaxSnapshotBytes { get; init; } = GroupSnapshotStore.DefaultMaxSnapshotBytes;

    /// <summary>Gets the longest time the leader waits for the answer to one transfer message; 10 seconds unless set.</summary>
    /// <remarks>It stays below <see cref="IdleTimeout" />, so a step the leader gave up on does not outlive the ticket it belongs to by much.</remarks>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is not positive.</exception>
    internal TimeSpan StepTimeout
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            field = value;
        }
    } = TimeSpan.FromSeconds(10);

    /// <summary>Gets the largest number of keys one wipe step examines; 1024 unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The step is not positive.</exception>
    internal int WipeStepKeys
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            field = value;
        }
    } = 1024;

    /// <summary>Explains why the identity or declared snapshot of a begin is unusable.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <returns>The validation failure, or <see langword="null" /> when the transfer id and image length are usable.</returns>
    internal string? ValidationFailure(in ReplicaTransferHeader header, in ReplicaTransferBaseline baseline) => true switch
    {
        _ when string.IsNullOrWhiteSpace(header.TransferId) => "missing-transfer-id",
        _ when baseline.SnapshotBytes <= 0 => "nonpositive-snapshot-length",
        _ when baseline.SnapshotBytes > MaxSnapshotBytes => "snapshot-length-exceeds-limit",
        _ => null,
    };
}
