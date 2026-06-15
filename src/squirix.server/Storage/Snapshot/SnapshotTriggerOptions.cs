using System;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Configuration for time/volume based snapshot triggers and throttling guards driven by journal growth
/// and latency SLOs. All thresholds are evaluated cooperatively: a snapshot is eligible when at least
/// one trigger is satisfied and no active throttling guard is engaged.
/// </summary>
/// <remarks>
///     <para>
///     Typical triggering conditions:
///     <list type="bullet">
///         <item>
///             <description><see cref="SnapshotInterval" /> elapsed since the last snapshot.</description>
///         </item>
///         <item>
///             <description><see cref="SnapshotEveryNOps" /> operations have been applied since the last snapshot.</description>
///         </item>
///         <item>
///             <description><see cref="SnapshotEveryNBytes" /> of journal have been appended since the last snapshot.</description>
///         </item>
///     </list>
///     </para>
///     <para>
///     Throttling guards may suppress otherwise-eligible snapshots:
///     <list type="bullet">
///         <item>
///             <description><see cref="JournalGrowthThrottleBytes" /> requires a minimum journal delta before allowing a snapshot.</description>
///         </item>
///         <item>
///             <description>
///             Latency SLO breaches (<see cref="LatencySloMilliseconds" />) suppress snapshots for
///             <see cref="LatencyThrottleDuration" />.
///             </description>
///         </item>
///     </list>
///     </para>
/// </remarks>
internal sealed class SnapshotTriggerOptions
{
    public SnapshotTriggerOptions()
    {
        LatencyThrottleDuration = TimeSpan.FromSeconds(10);
        MinGapBetweenSnapshots = TimeSpan.FromMinutes(1);
        SnapshotEveryNBytes = 128L * 1024 * 1024;
        SnapshotEveryNOps = 250_000;
        SnapshotInterval = TimeSpan.FromMinutes(5);
    }

    /// <summary>
    /// Gets a value indicating whether snapshot triggering is enabled. When <c>false</c>, all snapshot decisions are disabled regardless of thresholds.
    /// Default is <c>true</c>.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Gets the minimum journal byte delta required before a snapshot is allowed, even when other triggers are satisfied.
    /// Default is 0 (disabled).
    /// </summary>
    public long JournalGrowthThrottleBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalGrowthThrottleBytes cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the latency SLO for journal append operations, in milliseconds.
    /// If the observed p95 (or chosen percentile) exceeds this value within the evaluation window,
    /// snapshot attempts are throttled for <see cref="LatencyThrottleDuration" />. Default is 0 (disabled).
    /// </summary>
    public double LatencySloMilliseconds
    {
        get;
        init
        {
            if (value < 0 || double.IsNaN(value) || double.IsInfinity(value))
                throw new ArgumentOutOfRangeException(nameof(value), value, "LatencySloMilliseconds must be a finite non-negative value.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the duration to suppress snapshot attempts after a latency SLO breach.
    /// Default is 10 seconds.
    /// </summary>
    public TimeSpan LatencyThrottleDuration
    {
        get;
        init
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "LatencyThrottleDuration cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the debounce guard: a minimum gap enforced between consecutive snapshots even if triggers fire back-to-back.
    /// Default is 1 minute.
    /// </summary>
    public TimeSpan MinGapBetweenSnapshots
    {
        get;
        init
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MinGapBetweenSnapshots cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the journal-size trigger: snapshot becomes eligible after at least this many bytes have been appended to the journal
    /// since the previous snapshot. Default is 128 MiB.
    /// </summary>
    public long SnapshotEveryNBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotEveryNBytes cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the operation-count trigger: snapshot becomes eligible after at least this many mutating operations
    /// have been applied since the previous snapshot. Default is 250,000.
    /// </summary>
    public long SnapshotEveryNOps
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotEveryNOps cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the time-based trigger interval: minimum elapsed time since the previous snapshot to consider a new snapshot.
    /// Default is 5 minutes.
    /// </summary>
    public TimeSpan SnapshotInterval
    {
        get;
        init
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotInterval must be greater than zero.");

            field = value;
        }
    }
}
