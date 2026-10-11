using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The time a new leader of one group may wait for authority, and the wait that follows when it gives the group up.</summary>
/// <remarks>
/// <para>
/// The window is the authority timeout doubled for each consecutive leadership of this node that ended without authority, at most three
/// times, and it restarts from the timeout once the node gets authority. The time runs from the moment the node took the leadership.
/// </para>
/// <para>
/// A leadership that never had authority admitted no client write and served no quorum read, so ending it loses nothing acknowledged; its
/// leader-term entry and any recovered tail stay in the log, and the next leader commits or truncates them, as after a crash. Authority is
/// granted only by the election driver and only while the role is leader in that term, so a commit of the entry that races the give-up
/// grants nothing. Not thread safe: the election driver of the group is its only caller.
/// </para>
/// </remarks>
internal sealed class AuthorityDeadline
{
    private const int MaxDoublings = 3;

    private readonly TimeProvider _clock;
    private readonly ElectionTimerOptions _options;
    private TimeSpan _abstain;
    private int _failures;
    private long _since;

    /// <summary>Initializes a new instance of the <see cref="AuthorityDeadline" /> class.</summary>
    /// <param name="clock">The clock of the group.</param>
    /// <param name="options">The election timing.</param>
    internal AuthorityDeadline(TimeProvider clock, ElectionTimerOptions options)
    {
        _clock = clock;
        _options = options;
    }

    /// <summary>Starts the wait of a leadership that was just taken.</summary>
    internal void Begin() => _since = _clock.GetTimestamp();

    /// <summary>Restarts the windows from the authority timeout: the node got authority.</summary>
    internal void Granted() => _failures = 0;

    /// <summary>Tells whether a leader gave up waiting.</summary>
    /// <param name="authorized">Whether the leader has authority, which no deadline ends.</param>
    /// <returns><see langword="true" /> when the leader has no authority and its window elapsed.</returns>
    internal bool IsOverdue(bool authorized) => !authorized && _clock.GetElapsedTime(_since) >= Window();

    /// <summary>Records that the leader gave the group up and lengthens the next window.</summary>
    /// <returns>How long the leader waited.</returns>
    internal TimeSpan GiveUp()
    {
        var waited = _clock.GetElapsedTime(_since);
        if (_failures < MaxDoublings)
            _failures++;

        _abstain = _options.ElectionTimeout + _options.MaxJitter + (_options.VoteRpcTimeout * 2);
        return waited;
    }

    /// <summary>Takes the extra wait before the next campaign that follows a give-up; it applies once.</summary>
    /// <returns>The extra wait, or zero when none is pending.</returns>
    internal TimeSpan TakeAbstain()
    {
        var abstain = _abstain;
        _abstain = TimeSpan.Zero;
        return abstain;
    }

    private TimeSpan Window() => _options.AuthorityTimeout * (1 << _failures);
}
