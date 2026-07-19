using System;

namespace Squirix.Server.Node.Services;

/// <summary>Resolved runtime idempotency store limits for in-memory mutation replay records.</summary>
internal sealed record IdempotencyOptions
{
    /// <summary>Initializes a new instance of the <see cref="IdempotencyOptions" /> class.</summary>
    internal IdempotencyOptions()
    {
        Retention = TimeSpan.FromMinutes(15);
        MaxInFlightRecords = 65_536;
        BackgroundSweepInterval = TimeSpan.FromMinutes(1);
    }

    /// <summary>Gets the interval for background expiry sweeps in addition to lazy per-access sweeps.</summary>
    internal TimeSpan BackgroundSweepInterval { get; init; }

    /// <summary>Gets the maximum number of in-flight idempotency records retained in memory.</summary>
    internal int MaxInFlightRecords { get; init; }

    /// <summary>Gets how long successful mutation outcomes remain replayable.</summary>
    internal TimeSpan Retention { get; init; }

    /// <summary>Validates configuration.</summary>
    /// <exception cref="InvalidOperationException">Thrown when retention, capacity, or sweep interval is invalid.</exception>
    internal void Validate()
    {
        if (Retention <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency Retention must be positive.");

        if (MaxInFlightRecords <= 0)
            throw new InvalidOperationException("Idempotency MaxInFlightRecords must be positive.");

        if (BackgroundSweepInterval <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency BackgroundSweepInterval must be positive.");
    }
}
