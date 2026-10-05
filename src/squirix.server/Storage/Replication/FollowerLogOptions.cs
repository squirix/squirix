using System;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Optional construction settings for a durable follower log.</summary>
[Immutable]
internal sealed class FollowerLogOptions
{
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    /// <summary>Initializes a new instance of the <see cref="FollowerLogOptions" /> class with the default shutdown budget.</summary>
    internal FollowerLogOptions()
    {
        ShutdownBudget = DefaultShutdownBudget;
    }

    /// <summary>Gets the fault hooks used by failure-injection tests.</summary>
    internal IFollowerLogFaultHooks? FaultHooks { get; init; }

    /// <summary>Gets the time source used by idempotency retention only; the shutdown budget never follows it.</summary>
    internal TimeProvider? TimeProvider { get; init; }

    /// <summary>Gets the time source of the shutdown budget; the system clock unless set.</summary>
    /// <remarks>Test seam: hosted composition leaves it on the system clock, so a host clock that never advances cannot park a dispose.</remarks>
    internal TimeProvider ShutdownTimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the maximum number of retained idempotency records.</summary>
    internal int IdempotencyCapacity { get; init; } = 1024;

    /// <summary>Gets the idempotency retention window; <see langword="null" /> selects <see cref="GroupIdempotencyState.DefaultRetention" />, <see cref="TimeSpan.Zero" /> means explicit immediate expiration.</summary>
    internal TimeSpan? IdempotencyRetention { get; init; }

    /// <summary>Gets the maximum accepted snapshot size in bytes.</summary>
    internal int MaxSnapshotBytes { get; init; } = GroupSnapshotStore.DefaultMaxSnapshotBytes;

    /// <summary>Gets the longest wait for an in-flight durable operation on dispose; 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The shutdown budget must be greater than zero.");

            field = value;
        }
    }
}
