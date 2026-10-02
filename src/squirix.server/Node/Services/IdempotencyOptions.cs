using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Resolved runtime idempotency store limits for in-memory mutation replay records.</summary>
[Immutable]
internal sealed record IdempotencyOptions
{
    /// <summary>Initializes a new instance of the <see cref="IdempotencyOptions" /> class.</summary>
    internal IdempotencyOptions()
    {
        Retention = TimeSpan.FromMinutes(15);
        MaxInFlightRecords = ushort.MaxValue + 1;
        MinRetention = TimeSpan.FromMinutes(1);
        BackgroundSweepInterval = TimeSpan.FromMinutes(1);
    }

    /// <summary>Gets the interval for background expiry sweeps in addition to lazy per-access sweeps.</summary>
    internal TimeSpan BackgroundSweepInterval { get; init; }

    /// <summary>Gets the maximum number of in-flight idempotency records retained in memory.</summary>
    internal int MaxInFlightRecords { get; init; }

    /// <summary>
    /// Gets how long a successful mutation outcome stays replayable under capacity pressure: an older outcome may be evicted to admit
    /// a new operation, and when none is that old the new operation is refused.
    /// </summary>
    internal TimeSpan MinRetention { get; init; }

    /// <summary>Gets how long successful mutation outcomes remain replayable.</summary>
    internal TimeSpan Retention { get; init; }

    /// <summary>Validates configuration.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a retention, the capacity, or the sweep interval is invalid.</exception>
    internal void Validate()
    {
        if (Retention <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency Retention must be positive.");

        if (MinRetention < TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency MinRetention must not be negative.");

        if (MaxInFlightRecords <= 0)
            throw new InvalidOperationException("Idempotency MaxInFlightRecords must be positive.");

        if (BackgroundSweepInterval <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency BackgroundSweepInterval must be positive.");
    }
}
