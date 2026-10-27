using System.Collections.Generic;

namespace Squirix.Server.Benchmarks;

/// <summary>The measured result of one <see cref="StallCellSpec" />.</summary>
internal sealed class StallCellResult
{
    /// <summary>Initializes a new instance of the <see cref="StallCellResult" /> class.</summary>
    /// <param name="cell">The measured cell.</param>
    internal StallCellResult(StallCellSpec cell)
    {
        Cell = cell;
    }

    /// <summary>Gets or sets the baseline throughput before the stall, in operations per second.</summary>
    internal double BaselineOps { get; set; }

    /// <summary>Gets the measured cell.</summary>
    internal StallCellSpec Cell { get; }

    /// <summary>Gets the number of requests per <see cref="StallOutcome" /> value among those in flight during the stall.</summary>
    internal int[] Counts { get; } = new int[StallOutcome.Count];

    /// <summary>Gets or sets the milliseconds from the stall end until the orphan backlog drained.</summary>
    internal double DrainMs { get; set; } = double.NaN;

    /// <summary>Gets or sets the p50 wait of a failed request, in milliseconds.</summary>
    internal double FailureWaitP50Ms { get; set; }

    /// <summary>Gets or sets the p99 wait of a failed request, in milliseconds.</summary>
    internal double FailureWaitP99Ms { get; set; }

    /// <summary>Gets or sets the longest wait of a failed request, in milliseconds.</summary>
    internal double FailureWaitMaxMs { get; set; }

    /// <summary>Gets the fail-fast comparisons.</summary>
    internal List<StallFailFast> FailFast { get; } = [];

    /// <summary>Gets or sets the number of gate holds longer than one second.</summary>
    internal int HoldsOverSecond { get; set; }

    /// <summary>Gets or sets the number of timed-out requests that committed afterwards.</summary>
    internal int LaterCommitted { get; set; }

    /// <summary>Gets or sets the longest gate hold in milliseconds.</summary>
    internal double MaxHoldMs { get; set; }

    /// <summary>Gets or sets the peak managed heap growth over the baseline, in megabytes.</summary>
    internal double ManagedGrowthMb { get; set; }

    /// <summary>Gets or sets the error descriptions other than cancellations.</summary>
    internal string[] OtherErrors { get; set; } = [];

    /// <summary>Gets or sets the peak number of requests appended and waiting for durability.</summary>
    internal int PeakAppended { get; set; }

    /// <summary>Gets or sets the peak journal I/O age the stall probe reported, in milliseconds.</summary>
    internal double PeakIoAgeMs { get; set; }

    /// <summary>Gets or sets the peak number of requests the server ran.</summary>
    internal int PeakInflight { get; set; }

    /// <summary>Gets or sets the peak number of requests the client gave up on that the server still ran.</summary>
    internal int PeakOrphans { get; set; }

    /// <summary>Gets or sets the peak number of requests waiting for the gate.</summary>
    internal int PeakWaiting { get; set; }

    /// <summary>Gets or sets the milliseconds from the stall end until throughput was back to normal.</summary>
    internal double RecoveryMs { get; set; } = double.NaN;

    /// <summary>Gets or sets the stall duration in milliseconds.</summary>
    internal double StallMs { get; set; }

    /// <summary>Gets or sets the number of requests in flight during the stall.</summary>
    internal int WindowRequests { get; set; }

    /// <summary>Gets or sets the peak working set growth over the baseline, in megabytes.</summary>
    internal double WorkingSetGrowthMb { get; set; }
}
