using System;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>A deadline converted once into a budget that counts down on the monotonic timestamp of its clock.</summary>
/// <param name="Clock">The clock the budget counts down on.</param>
/// <param name="Budget">The budget left when the deadline was pushed.</param>
/// <param name="StartedTimestamp">The monotonic timestamp of the push.</param>
internal readonly record struct DeadlineBudget(TimeProvider Clock, TimeSpan Budget, long StartedTimestamp)
{
    /// <summary>Gets the budget left, negative once the deadline passed.</summary>
    internal TimeSpan Remaining => Budget - Clock.GetElapsedTime(StartedTimestamp);

    /// <summary>Starts the budget left before <paramref name="deadlineUtc" /> on <paramref name="clock" />.</summary>
    /// <param name="deadlineUtc">The absolute deadline in UTC.</param>
    /// <param name="clock">The clock the deadline is expressed in.</param>
    /// <returns>The started budget.</returns>
    internal static DeadlineBudget Start(DateTime deadlineUtc, TimeProvider clock) => new(clock, deadlineUtc - clock.GetUtcNow().UtcDateTime, clock.GetTimestamp());
}
