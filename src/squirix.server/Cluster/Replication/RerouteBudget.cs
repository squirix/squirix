using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>One absolute deadline shared by a server reroute and its transport retries, with at most one reroute.</summary>
/// <remarks>
/// The budget is created once per logical operation from a single absolute deadline. Transport retries
/// observe the same deadline through the ambient RPC deadline context instead of minting their own,
/// so retry counters never multiply across the reroute and transport layers.
/// </remarks>
[ThreadSafe]
[SuppressMessage("Usage", "MA0182:Internal type is apparently never used", Justification = "Test-only activation seam until failover activation wires the reroute budget in a follow-up milestone.")]
internal sealed class RerouteBudget
{
    private readonly TimeProvider _timeProvider;
    private int _rerouteConsumed;

    /// <summary>Initializes a new instance of the <see cref="RerouteBudget" /> class.</summary>
    /// <param name="deadlineUtc">The single absolute deadline for the reroute and its transport retries.</param>
    /// <param name="timeProvider">The time source reading the clock; <see langword="null" /> selects <see cref="TimeProvider.System" />.</param>
    internal RerouteBudget(DateTimeOffset deadlineUtc, TimeProvider? timeProvider = null)
    {
        DeadlineUtc = deadlineUtc;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Gets the single absolute deadline for the operation.</summary>
    /// <returns>The absolute deadline.</returns>
    private DateTimeOffset DeadlineUtc { get; }

    /// <summary>Gets the time remaining until the absolute deadline.</summary>
    /// <returns>The remaining budget; negative when the deadline already passed.</returns>
    internal TimeSpan GetRemaining() => DeadlineUtc - _timeProvider.GetUtcNow();

    /// <summary>Determines whether the absolute deadline already passed.</summary>
    /// <returns><see langword="true" /> when no budget remains; otherwise <see langword="false" />.</returns>
    internal bool HasExpired() => GetRemaining() <= TimeSpan.Zero;

    /// <summary>Consumes the single reroute attempt.</summary>
    /// <returns><see langword="true" /> on the first call; <see langword="false" /> once the reroute was consumed.</returns>
    internal bool TryConsumeReroute() => Interlocked.Exchange(ref _rerouteConsumed, 1) == 0;
}
