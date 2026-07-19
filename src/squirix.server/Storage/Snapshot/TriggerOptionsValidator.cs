using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
internal sealed class TriggerOptionsValidator : IValidateOptions<TriggerOptions>
{
    public ValidateOptionsResult Validate(string? name, TriggerOptions options)
    {
        var failures = new List<string>();
        if (options.SnapshotInterval <= TimeSpan.Zero)
            failures.Add("Snapshot SnapshotInterval must be greater than zero.");
        if (options.SnapshotEveryNOps < 0)
            failures.Add("Snapshot SnapshotEveryNOps cannot be negative.");
        if (options.SnapshotEveryNBytes < 0)
            failures.Add("Snapshot SnapshotEveryNBytes cannot be negative.");
        if (options.MinGapBetweenSnapshots < TimeSpan.Zero)
            failures.Add("Snapshot MinGapBetweenSnapshots cannot be negative.");
        if (options.JournalGrowthThrottleBytes < 0)
            failures.Add("Snapshot JournalGrowthThrottleBytes cannot be negative.");
        if (options.LatencySloMilliseconds < 0 || double.IsNaN(options.LatencySloMilliseconds) || double.IsInfinity(options.LatencySloMilliseconds))
            failures.Add("Snapshot LatencySloMilliseconds must be a finite non-negative value.");
        if (options.LatencyThrottleDuration < TimeSpan.Zero)
            failures.Add("Snapshot LatencyThrottleDuration cannot be negative.");

        return OptionsValidator.ToResult(failures);
    }
}
