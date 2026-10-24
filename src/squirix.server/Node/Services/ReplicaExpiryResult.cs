using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.Node.Services;

/// <summary>The result of one key of a batch expiry.</summary>
/// <param name="Status">What the batch decided for the key.</param>
/// <param name="Entry">The stored entry when the key is live; otherwise <see langword="null" />.</param>
/// <param name="Failure">The failure of the tombstone when the key is <see cref="ReplicaExpiryStatus.Failed" />; otherwise <see langword="null" />.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaExpiryResult(ReplicaExpiryStatus Status, NodeCacheEntry<object?>? Entry, Exception? Failure)
{
    /// <summary>Gets the result of a key that holds no entry.</summary>
    internal static ReplicaExpiryResult Absent => new(ReplicaExpiryStatus.Absent, null, null);

    /// <summary>Gets the result of a key whose tombstone is committed.</summary>
    internal static ReplicaExpiryResult Tombstoned => new(ReplicaExpiryStatus.Tombstoned, null, null);

    /// <summary>Creates the result of a key that holds a live entry.</summary>
    /// <param name="entry">The stored entry.</param>
    /// <returns>The result.</returns>
    internal static ReplicaExpiryResult Live(NodeCacheEntry<object?> entry) => new(ReplicaExpiryStatus.Live, entry, null);

    /// <summary>Creates the result of a key whose tombstone is not committed.</summary>
    /// <param name="failure">The client failure of the tombstone.</param>
    /// <returns>The result.</returns>
    internal static ReplicaExpiryResult Failed(Exception failure) => new(ReplicaExpiryStatus.Failed, null, failure);
}
