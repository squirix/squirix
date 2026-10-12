using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Settings of the receiver of a state transfer.</summary>
[Immutable]
internal sealed class ReplicaTransferOptions
{
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

    /// <summary>Gets the largest published snapshot image the receiver buffers.</summary>
    internal int MaxSnapshotBytes { get; init; } = GroupSnapshotStore.DefaultMaxSnapshotBytes;

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

    /// <summary>Tells whether the identity and the declared snapshot of a begin are usable at all.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <returns><see langword="true" /> when the transfer id is set and the declared image length is positive and within <see cref="MaxSnapshotBytes" />.</returns>
    internal bool Admits(in ReplicaTransferHeader header, in ReplicaTransferBaseline baseline) =>
        !string.IsNullOrWhiteSpace(header.TransferId) && baseline.SnapshotBytes > 0 && baseline.SnapshotBytes <= MaxSnapshotBytes;
}
