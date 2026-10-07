using System;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Stable lower-case names of <see cref="ReplicaCatchUpOutcome" /> values for logs and metric labels.</summary>
internal static class ReplicaCatchUpOutcomeNames
{
    /// <summary>Returns the stable name of an outcome.</summary>
    /// <param name="outcome">The catch-up outcome.</param>
    /// <returns>The snake-case outcome name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome is not a named value.</exception>
    internal static string Of(ReplicaCatchUpOutcome outcome) => outcome switch
    {
        ReplicaCatchUpOutcome.CaughtUp => "caught_up",
        ReplicaCatchUpOutcome.Compacted => "compacted",
        ReplicaCatchUpOutcome.Diverged => "diverged",
        ReplicaCatchUpOutcome.StaleTerm => "stale_term",
        ReplicaCatchUpOutcome.Refused => "refused",
        ReplicaCatchUpOutcome.Unreachable => "unreachable",
        ReplicaCatchUpOutcome.Corrupt => "corrupt",
        ReplicaCatchUpOutcome.Aborted => "aborted",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported replica catch-up outcome."),
    };
}
