using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fixed configuration of the replica group elections: the timeouts, the heartbeat, and the jitter that breaks split votes.</summary>
/// <remarks>
/// The defaults keep a cluster whose nodes start a few hundred milliseconds apart in its provisional term: an election timeout of half a
/// second deposed the owner of a group before its peers answered and elected a needless second term in half of the measured three-node
/// starts, while one second elected none. A failover then waits one to two seconds for the timeout and the jitter before it campaigns.
/// </remarks>
[Immutable]
internal sealed class ElectionTimerOptions
{
    /// <summary>The longest configurable wait for a leader; a request deadline is far shorter.</summary>
    internal static readonly TimeSpan MaxLeaderWaitTimeout = TimeSpan.FromMinutes(1);

    private static readonly TimeSpan DefaultApplyTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan DefaultAuthorityTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan DefaultElectionTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan DefaultMaxJitter = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultVoteRpcTimeout = TimeSpan.FromMilliseconds(250);

    /// <summary>Gets the time a new leader may wait for authority before it gives the group up; it doubles for each give-up of the same node.</summary>
    /// <remarks>
    /// The window starts after the first promotion attempt returned without authority. A healthy promotion needs the launch probes (up to 1
    /// second), one readiness retry gap (up to 5 seconds), and the catch-up of the followers a majority needs, so the bound is well above
    /// it. A leader interrupted while a follower is still catching up is elected again and continues with a doubled window, the catch-up
    /// progress being kept on the follower.
    /// </remarks>
    internal TimeSpan AuthorityTimeout { get; init; } = DefaultAuthorityTimeout;

    /// <summary>Gets the time a leader with authority may stand still on a committed entry it cannot apply before it gives the group up.</summary>
    /// <remarks>
    /// The window runs while the applied index of the group lags the entries a majority backs and does not move; any progress restarts it, and it
    /// does not double. The bound holds at least six readiness retries at their longest gap of 5 seconds, so a transient fault of the journal or
    /// of memory clears first, and it is well above the 5 second budget of one commit. It is separate from <see cref="AuthorityTimeout" />,
    /// whose window it never changes.
    /// </remarks>
    internal TimeSpan ApplyTimeout { get; init; } = DefaultApplyTimeout;

    /// <summary>Gets the time without leader contact after which a follower starts an election; also the window of the leader quorum check.</summary>
    internal TimeSpan ElectionTimeout { get; init; } = DefaultElectionTimeout;

    /// <summary>Gets the time between two heartbeats of a leader; it must stay well below <see cref="ElectionTimeout" />.</summary>
    internal TimeSpan HeartbeatInterval { get; init; } = DefaultHeartbeatInterval;

    /// <summary>Gets the seed of the election jitter; every group mixes its identifier in, so groups draw different delays.</summary>
    /// <remarks>Drawn once per options instance unless set, so nodes differ while a test can pin it for a deterministic run.</remarks>
    internal ulong JitterSeed { get; init; } = BinaryPrimitives.ReadUInt64LittleEndian(RandomNumberGenerator.GetBytes(sizeof(ulong)));

    /// <summary>Gets the longest wait of an entry node for a leader of a served group that has none known.</summary>
    /// <remarks>
    /// <see cref="LeaderWaitTimeoutOverride" /> when set; otherwise <see cref="ElectionTimeout" /> plus <see cref="MaxJitter" />, the longest a
    /// follower waits before it campaigns. The remaining deadline of the request caps it further.
    /// </remarks>
    internal TimeSpan LeaderWaitTimeout => LeaderWaitTimeoutOverride ?? ElectionTimeout + MaxJitter;

    /// <summary>Initializes the explicit longest wait for a leader; <see langword="null" /> keeps the default of <see cref="LeaderWaitTimeout" />.</summary>
    /// <remarks>
    /// It must be positive and at most <see cref="MaxLeaderWaitTimeout" />; <see cref="System.Threading.Timeout.InfiniteTimeSpan" /> is
    /// refused, because a wait for a leader is always bounded. <see cref="EnsureValidLeaderWait" /> checks it once, when the leader table
    /// is built.
    /// </remarks>
    internal TimeSpan? LeaderWaitTimeoutOverride { private get; init; }

    /// <summary>Gets the largest random delay added to <see cref="ElectionTimeout" /> each time a follower arms its election.</summary>
    internal TimeSpan MaxJitter { get; init; } = DefaultMaxJitter;

    /// <summary>Gets the longest wait for one pre-vote or vote reply; an unanswered voter counts as a refusal, and a round ends earlier once its outcome is decided.</summary>
    internal TimeSpan VoteRpcTimeout { get; init; } = DefaultVoteRpcTimeout;

    /// <summary>Checks that the wait for a leader of the options is bounded: positive and at most <see cref="MaxLeaderWaitTimeout" />.</summary>
    /// <param name="options">The options to check.</param>
    /// <exception cref="ArgumentOutOfRangeException">The wait is zero, negative, infinite, or above <see cref="MaxLeaderWaitTimeout" />.</exception>
    internal static void EnsureValidLeaderWait(ElectionTimerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var wait = options.LeaderWaitTimeout;
        if (wait <= TimeSpan.Zero || wait > MaxLeaderWaitTimeout)
            throw new ArgumentOutOfRangeException(nameof(options), wait, "The wait for a leader must be positive and bounded.");
    }
}
