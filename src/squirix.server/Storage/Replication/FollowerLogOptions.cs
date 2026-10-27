using System;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Construction settings for a durable follower log.</summary>
[Immutable]
internal sealed class FollowerLogOptions
{
    /// <summary>The share of a snapshot limit smaller than four times the headroom that the derived byte bound leaves free: a quarter.</summary>
    private const int SmallLimitHeadroomDivisor = 4;

    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    /// <summary>Initializes a new instance of the <see cref="FollowerLogOptions" /> class with the default shutdown budget.</summary>
    internal FollowerLogOptions()
    {
        ShutdownBudget = DefaultShutdownBudget;
    }

    /// <summary>Gets the fault hooks used by failure-injection tests.</summary>
    internal IFollowerLogFaultHooks? FaultHooks { get; init; }

    /// <summary>Gets the time source used by idempotency retention only; the shutdown budget never follows it.</summary>
    internal required TimeProvider TimeProvider { get; init; }

    /// <summary>Gets the time source of the shutdown budget.</summary>
    /// <remarks>Hosted composition sets the system clock, so a host clock that never advances cannot park a dispose.</remarks>
    internal required TimeProvider ShutdownTimeProvider { get; init; }

    /// <summary>Gets the maximum number of retained idempotency records.</summary>
    internal int IdempotencyCapacity { get; init; } = 1024;

    /// <summary>Gets the idempotency retention window; <see langword="null" /> selects <see cref="GroupIdempotencyState.DefaultRetention" />, <see cref="TimeSpan.Zero" /> means explicit immediate expiration.</summary>
    internal TimeSpan? IdempotencyRetention { get; init; }

    /// <summary>
    /// Gets the minimum retention: a full idempotency table gives up its oldest resolved outcome to a new operation only once that outcome is this old.
    /// <see langword="null" /> selects <see cref="IdempotencyRetention" />, so no unexpired outcome is evicted.
    /// </summary>
    internal TimeSpan? IdempotencyMinRetention { get; init; }

    /// <summary>
    /// Initializes the most bytes the idempotency outcomes are charged before a new operation needs an older outcome to make way;
    /// <see langword="null" /> derives it from <see cref="MaxSnapshotBytes" /> (see <see cref="ResolveIdempotencyMaxOutcomeBytes" />).
    /// </summary>
    /// <remarks>
    /// A value set here must hold one outcome and stay below <see cref="MaxSnapshotBytes" />, or be <see cref="long.MaxValue" />, which
    /// turns the byte bound off so a test can show what the bound prevents.
    /// </remarks>
    internal long? IdempotencyMaxOutcomeBytes { private get; init; }

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

    /// <summary>Returns the byte bound of the idempotency outcomes: the value set, checked against the snapshot limit, or one derived from it.</summary>
    /// <returns>The byte bound.</returns>
    /// <exception cref="InvalidOperationException">The value set cannot hold an outcome without a result or is not below <see cref="MaxSnapshotBytes" />.</exception>
    /// <remarks>
    /// The derived bound leaves <see cref="GroupIdempotencyState.OutcomeHeadroomBytes" /> of the snapshot limit free, or a quarter of a smaller
    /// limit, which gives <see cref="GroupIdempotencyState.DefaultMaxOutcomeBytes" /> for the default limit. The derived bound never drops
    /// below <see cref="GroupSnapshotStore.MinOutcomeEncodedLength" />, so one outcome is always admitted; a snapshot limit that small
    /// still refuses every snapshot.
    /// </remarks>
    internal long ResolveIdempotencyMaxOutcomeBytes()
    {
        var derived = MaxSnapshotBytes - Math.Min(GroupIdempotencyState.OutcomeHeadroomBytes, MaxSnapshotBytes / SmallLimitHeadroomDivisor);
        return IdempotencyMaxOutcomeBytes switch
        {
            null => Math.Max(derived, GroupSnapshotStore.MinOutcomeEncodedLength),
            long.MaxValue => long.MaxValue,
            { } bound when bound >= GroupSnapshotStore.MinOutcomeEncodedLength && bound < MaxSnapshotBytes => bound,
            { } bound => throw new InvalidOperationException(
                $"The idempotency outcome byte bound {bound} must hold one outcome ({GroupSnapshotStore.MinOutcomeEncodedLength} bytes) and stay below the snapshot limit ({MaxSnapshotBytes} bytes)."),
        };
    }
}
