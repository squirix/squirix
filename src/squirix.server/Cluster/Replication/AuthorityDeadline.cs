using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The time a new leader of one group may wait for authority, and the wait that follows when it gives the group up.</summary>
/// <remarks>
/// <para>
/// The window starts when the first promotion attempt of a leadership returned without authority, so it measures the time spent leading
/// and retrying, not the time inside the first start. It is the authority timeout doubled for each give-up of this node since it last
/// had authority, at most three times. A leadership that is deposed without authority, by a higher term or a silent majority, neither
/// counts as a give-up nor restarts the count, and following another leader does not restart it either.
/// </para>
/// <para>
/// After a give-up the node does not campaign before a point in time that starts once the retirement of the leadership completed and lies
/// the election timeout, the largest jitter, and two vote timeouts later, so every other member's timer and one pre-vote and one vote
/// round fit before it campaigns again. Once the point has passed it has no effect.
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
    private long _abstainFrom;
    private bool _abstainPending;
    private bool _abstaining;
    private bool _authorized;
    private int _failures;
    private long _since;
    private bool _waiting;

    /// <summary>Initializes a new instance of the <see cref="AuthorityDeadline" /> class.</summary>
    /// <param name="clock">The clock of the group.</param>
    /// <param name="options">The election timing.</param>
    internal AuthorityDeadline(TimeProvider clock, ElectionTimerOptions options)
    {
        _clock = clock;
        _options = options;
    }

    /// <summary>Gets how long the node still may not campaign after it gave a group up.</summary>
    /// <returns>The time left; zero when no abstain runs or it has passed.</returns>
    internal TimeSpan AbstainLeft()
    {
        var left = _abstaining ? AbstainLength() - _clock.GetElapsedTime(_abstainFrom) : TimeSpan.Zero;
        return left > TimeSpan.Zero ? left : TimeSpan.Zero;
    }

    /// <summary>Records that a promotion attempt returned without authority; the first one of a leadership starts its window.</summary>
    internal void AttemptFailed()
    {
        if (_waiting)
            return;

        _since = _clock.GetTimestamp();
        _waiting = true;
    }

    /// <summary>Starts a leadership: it has no authority yet, and its window is not running.</summary>
    internal void Begin()
    {
        _authorized = false;
        _waiting = false;
    }

    /// <summary>Records that the leadership got authority and restarts the windows from the authority timeout.</summary>
    internal void Granted()
    {
        _authorized = true;
        _failures = 0;
    }

    /// <summary>Records that the leader gave the group up and lengthens the next window.</summary>
    /// <returns>How long the leader waited.</returns>
    internal TimeSpan GiveUp()
    {
        var waited = _clock.GetElapsedTime(_since);
        if (_failures < MaxDoublings)
            _failures++;

        _abstainPending = true;
        return waited;
    }

    /// <summary>Tells whether the leader waited past its window without getting authority.</summary>
    /// <param name="progressing">Whether a state transfer to a follower made progress lately, which holds the deadline without moving the window.</param>
    /// <returns>
    /// <see langword="true" /> when the window runs, the leadership has no authority, no transfer is progressing, and the window elapsed. A transfer
    /// that stops making progress or ends without an admission leaves the window where it stood.
    /// </returns>
    internal bool IsOverdue(bool progressing) => _waiting && !_authorized && !progressing && _clock.GetElapsedTime(_since) >= Window();

    /// <summary>Records an answer of the retirement of the led group; the abstain after a give-up starts when it completed.</summary>
    /// <param name="retired">Whether the group is retired.</param>
    internal void RetireAnswered(bool retired)
    {
        if (!retired || !_abstainPending)
            return;

        _abstainPending = false;
        _abstaining = true;
        _abstainFrom = _clock.GetTimestamp();
    }

    private TimeSpan AbstainLength() => _options.ElectionTimeout + _options.MaxJitter + (_options.VoteRpcTimeout * 2);

    private TimeSpan Window() => _options.AuthorityTimeout * (1 << _failures);
}
