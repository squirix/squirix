using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Stable lower-case names of <see cref="ReplicaCatchUpOutcome" /> values for logs and metric labels.</summary>
internal static class ReplicaCatchUpOutcomeNames
{
    /// <summary>The name of every outcome; a test guards that every value is named.</summary>
    private static readonly FrozenDictionary<ReplicaCatchUpOutcome, string> Names = new Dictionary<ReplicaCatchUpOutcome, string>
    {
        [ReplicaCatchUpOutcome.CaughtUp] = "caught_up",
        [ReplicaCatchUpOutcome.Compacted] = "compacted",
        [ReplicaCatchUpOutcome.Diverged] = "diverged",
        [ReplicaCatchUpOutcome.StaleTerm] = "stale_term",
        [ReplicaCatchUpOutcome.Refused] = "refused",
        [ReplicaCatchUpOutcome.Unreachable] = "unreachable",
        [ReplicaCatchUpOutcome.Corrupt] = "corrupt",
        [ReplicaCatchUpOutcome.Aborted] = "aborted",
        [ReplicaCatchUpOutcome.Incomplete] = "incomplete",
        [ReplicaCatchUpOutcome.Unseeded] = "unseeded",
    }.ToFrozenDictionary();

    /// <summary>Returns the stable name of an outcome.</summary>
    /// <param name="outcome">The catch-up outcome.</param>
    /// <returns>The snake-case outcome name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome is not a named value.</exception>
    internal static string Of(ReplicaCatchUpOutcome outcome) =>
        Names.TryGetValue(outcome, out var name)
            ? name
            : throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported replica catch-up outcome.");
}
