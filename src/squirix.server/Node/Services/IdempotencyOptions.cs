using System;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>Resolved runtime idempotency store limits for in-memory mutation replay records.</summary>
[Immutable]
internal sealed record IdempotencyOptions
{
    /// <summary>
    /// The bytes one record is charged against <see cref="MaxRecordBytes" /> besides its response: the record object, the operation identifier
    /// and fingerprint strings (about 240 bytes for a 32-character identifier and a 64-character fingerprint), the response array header,
    /// the dictionary entry, and the entries of the expiry order, rounded up to a power of two.
    /// </summary>
    internal const long RecordOverheadBytes = 512;

    /// <summary>The most the default <see cref="MaxRecordBytes" /> grows to, however much memory the process has.</summary>
    internal const long DefaultMaxRecordBytesCeiling = 256L * 1024 * 1024;

    /// <summary>The least the default <see cref="MaxRecordBytes" /> shrinks to, however little memory the process has.</summary>
    internal const long DefaultMaxRecordBytesFloor = 32L * 1024 * 1024;

    /// <summary>The share of the memory available to the process the default <see cref="MaxRecordBytes" /> is made of, in percent.</summary>
    internal const int DefaultMemoryBudgetPercent = 5;

    /// <summary>The default <see cref="MaxInFlightRecords" />: twice what the largest default byte budget holds at the least charge, so the byte budget is the limit and the count a second guard.</summary>
    internal const int DefaultMaxInFlightRecords = 1 << 20;

    /// <summary>Initializes a new instance of the <see cref="IdempotencyOptions" /> class.</summary>
    internal IdempotencyOptions()
    {
        Retention = TimeSpan.FromMinutes(15);
        MaxInFlightRecords = DefaultMaxInFlightRecords;
        MaxRecordBytes = ResolveDefaultMaxRecordBytes(GcMemoryBudgetProvider.Instance);
        MinRetention = TimeSpan.FromMinutes(1);
        BackgroundSweepInterval = TimeSpan.FromMinutes(1);
    }

    /// <summary>Gets the interval for background expiry sweeps in addition to lazy per-access sweeps.</summary>
    internal TimeSpan BackgroundSweepInterval { get; init; }

    /// <summary>Gets the maximum number of in-flight idempotency records retained in memory.</summary>
    internal int MaxInFlightRecords { get; init; }

    /// <summary>Gets the bytes the retained records may be charged: <see cref="RecordOverheadBytes" /> plus the response of each record.</summary>
    internal long MaxRecordBytes { get; init; }

    /// <summary>
    /// Gets how long a successful mutation outcome stays replayable under capacity pressure: an older outcome may be evicted to admit
    /// a new operation, and when none is that old the new operation is refused.
    /// </summary>
    internal TimeSpan MinRetention { get; init; }

    /// <summary>Gets how long successful mutation outcomes remain replayable.</summary>
    internal TimeSpan Retention { get; init; }

    /// <summary>Resolves the default byte budget: a fraction of the memory available to the process, kept between a floor and a ceiling.</summary>
    /// <param name="budgetProvider">The source of the memory available to the process.</param>
    /// <returns>The budget in bytes.</returns>
    internal static long ResolveDefaultMaxRecordBytes(IMemoryBudgetProvider budgetProvider)
    {
        ArgumentNullException.ThrowIfNull(budgetProvider);

        var share = budgetProvider.GetTotalAvailableBytes() / 100 * DefaultMemoryBudgetPercent;
        return Math.Clamp(share, DefaultMaxRecordBytesFloor, DefaultMaxRecordBytesCeiling);
    }

    /// <summary>Validates configuration.</summary>
    /// <exception cref="InvalidOperationException">Thrown when a retention, the capacity, the byte budget, or the sweep interval is invalid.</exception>
    internal void Validate()
    {
        if (Retention <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency Retention must be positive.");

        if (MinRetention < TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency MinRetention must not be negative.");

        if (MaxInFlightRecords <= 0)
            throw new InvalidOperationException("Idempotency MaxInFlightRecords must be positive.");

        if (MaxRecordBytes <= 0)
            throw new InvalidOperationException("Idempotency MaxRecordBytes must be positive (SQUIRIX_IDEMPOTENCY_MAX_BYTES).");

        if (MaxRecordBytes < RecordOverheadBytes)
            throw new InvalidOperationException($"Idempotency MaxRecordBytes must be at least {RecordOverheadBytes} bytes, the charge of one record (SQUIRIX_IDEMPOTENCY_MAX_BYTES).");

        if (BackgroundSweepInterval <= TimeSpan.Zero)
            throw new InvalidOperationException("Idempotency BackgroundSweepInterval must be positive.");

        if (BackgroundSweepInterval > TimedWait.MaxDelay)
            throw new InvalidOperationException($"Idempotency BackgroundSweepInterval must not exceed {TimedWait.MaxDelay:c}, the longest period a timer supports.");
    }
}
