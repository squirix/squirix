using System;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>A deadline converted once into a budget that counts down on the monotonic timestamp of its clock.</summary>
/// <param name="Clock">The clock the budget counts down on.</param>
/// <param name="Budget">The budget left when the deadline was pushed.</param>
/// <param name="StartedTimestamp">The monotonic timestamp of the push.</param>
[Immutable]
internal readonly record struct DeadlineBudget(TimeProvider Clock, TimeSpan Budget, long StartedTimestamp)
{
    /// <summary>Gets the budget left, negative once the deadline passed.</summary>
    internal TimeSpan Remaining => Budget - Clock.GetElapsedTime(StartedTimestamp);

    /// <summary>Gets the absolute deadline to hand to gRPC now: the remaining budget from the current system time, never in the past.</summary>
    /// <remarks>
    /// gRPC turns an absolute deadline back into a timeout on the system clock when a call starts, so rebuilding it from the budget at that
    /// moment keeps the gRPC timer and the budget timer expiring together even after a wall-clock step.
    /// </remarks>
    internal DateTime ForwardDeadlineUtc
    {
        get
        {
            var remaining = Remaining;
            return TimeProvider.System.GetUtcNow().UtcDateTime + (remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        }
    }

    /// <summary>Starts the budget left before <paramref name="deadlineUtc" /> on <paramref name="clock" />.</summary>
    /// <param name="deadlineUtc">The absolute deadline in UTC.</param>
    /// <param name="clock">The clock the deadline is expressed in.</param>
    /// <returns>The started budget.</returns>
    internal static DeadlineBudget Start(DateTime deadlineUtc, TimeProvider clock) => new(clock, deadlineUtc - clock.GetUtcNow().UtcDateTime, clock.GetTimestamp());
}
