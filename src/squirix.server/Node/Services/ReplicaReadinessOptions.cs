using System;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Retry schedule of the follower verification run by <see cref="ReplicaGroupReadinessService" />.</summary>
/// <remarks>
/// A pending verification is retried after <see cref="InitialDelay" />, doubling up to <see cref="MaxDelay" />; a ready or blocked group is
/// re-checked at <see cref="MaxDelay" />. A follower queued for repair cuts any wait short.
/// </remarks>
[Immutable]
internal sealed class ReplicaReadinessOptions
{
    /// <summary>Gets the first retry delay of a pending verification; 250 milliseconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The delay is not positive.</exception>
    internal TimeSpan InitialDelay
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The initial verification delay must be greater than zero.");

            field = value;
        }
    } = TimeSpan.FromMilliseconds(250);

    /// <summary>Gets the longest delay between two verifications; 5 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The delay is not positive.</exception>
    internal TimeSpan MaxDelay
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The maximum verification delay must be greater than zero.");

            field = value;
        }
    } = TimeSpan.FromSeconds(5);
}
