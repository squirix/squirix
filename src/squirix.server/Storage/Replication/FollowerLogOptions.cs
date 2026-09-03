using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Optional construction settings for a durable follower log.</summary>
[Immutable]
internal sealed class FollowerLogOptions
{
    /// <summary>Gets the fault hooks used by failure-injection tests.</summary>
    internal IFollowerLogFaultHooks? FaultHooks { get; init; }

    /// <summary>Gets the time source used by idempotency retention.</summary>
    internal TimeProvider? TimeProvider { get; init; }

    /// <summary>Gets the maximum number of retained idempotency records.</summary>
    internal int IdempotencyCapacity { get; init; } = 1024;

    /// <summary>Gets the idempotency retention window; <see langword="null" /> selects <see cref="GroupIdempotencyState.DefaultRetention" />, <see cref="TimeSpan.Zero" /> means explicit immediate expiration.</summary>
    internal TimeSpan? IdempotencyRetention { get; init; }

    /// <summary>Gets the maximum accepted snapshot size in bytes.</summary>
    internal int MaxSnapshotBytes { get; init; } = GroupSnapshotStore.DefaultMaxSnapshotBytes;
}
