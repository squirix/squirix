using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>A deadline converted once into a budget that counts down on the monotonic timestamp of its clock.</summary>
/// <param name="Clock">The clock the budget counts down on.</param>
/// <param name="Budget">The budget left when the deadline was pushed.</param>
/// <param name="StartedTimestamp">The monotonic timestamp of the push.</param>
[Immutable]
internal readonly record struct ServerDeadlineBudget(TimeProvider Clock, TimeSpan Budget, long StartedTimestamp)
{
    /// <summary>Gets the budget left, negative once the deadline passed.</summary>
    internal TimeSpan Remaining => Budget - Clock.GetElapsedTime(StartedTimestamp);

    /// <summary>Starts the budget left before <paramref name="deadlineUtc" /> on <paramref name="clock" />.</summary>
    /// <param name="deadlineUtc">The absolute deadline in UTC.</param>
    /// <param name="clock">The clock the deadline is expressed in.</param>
    /// <returns>The started budget.</returns>
    internal static ServerDeadlineBudget Start(DateTime deadlineUtc, TimeProvider clock) => new(clock, deadlineUtc - clock.GetUtcNow().UtcDateTime, clock.GetTimestamp());

    /// <summary>Limits the budget to <paramref name="span" /> counted from the same start, never extending it.</summary>
    /// <param name="span">The longest budget left at the start of this budget.</param>
    /// <returns>This budget, or a copy with the budget reduced to <paramref name="span" />; the clock and the start timestamp are kept.</returns>
    internal ServerDeadlineBudget CappedAt(TimeSpan span) => span < Budget ? this with { Budget = span } : this;
}
