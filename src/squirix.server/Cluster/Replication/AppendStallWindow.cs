using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The window in which the appends of a leader to its own group log fail or hang without a success, as the election driver observes it tick by tick.</summary>
/// <remarks>
/// <para>
/// Two things end a leadership. A failed append, or a log that refused the last append as not ready, starts a window at the first tick that
/// sees it after the last success; the window survives quiet ticks, and it ends the leadership only while a failure is still visible in the
/// current tick. An append in flight counts only after it has itself stood still: it must be the same append (the same started-appends
/// counter) on two ticks in a row, and the timeout then runs from that second tick, so a healthy append caught mid-flush, whatever happened
/// before, never ends the leadership.
/// </para>
/// <para>
/// Any success clears both. The first observation of a leadership only records the counters, so failures before it never count. Not thread
/// safe: the election driver of the group is its only caller.
/// </para>
/// </remarks>
internal sealed class AppendStallWindow
{
    private readonly TimeProvider _clock;
    private ulong _appended;
    private ulong _failed;
    private long _flightSince;
    private bool _seeded;
    private long _since;
    private ulong _started;
    private bool _stalled;
    private bool _standing;
    private bool _visible;
    private bool _wasInFlight;

    /// <summary>Initializes a new instance of the <see cref="AppendStallWindow" /> class.</summary>
    /// <param name="clock">The clock of the group.</param>
    internal AppendStallWindow(TimeProvider clock)
    {
        _clock = clock;
    }

    /// <summary>Forgets the counters and the windows, so the next observation only records the counters.</summary>
    internal void Reset()
    {
        _seeded = false;
        _stalled = false;
        _standing = false;
        _visible = false;
        _wasInFlight = false;
    }

    /// <summary>Records what the appends did since the previous observation.</summary>
    /// <param name="progress">The progress of the local appends.</param>
    internal void Observe(in ReplicaAppendProgress progress)
    {
        var seeded = _seeded;
        var advanced = progress.Appended != _appended;
        var failedAgain = progress.Failed != _failed;
        var sameAppend = progress.InFlight && _wasInFlight && progress.Started == _started;
        _appended = progress.Appended;
        _failed = progress.Failed;
        _started = progress.Started;
        _wasInFlight = progress.InFlight;
        _seeded = true;
        _visible = false;
        if (!seeded || advanced)
        {
            _stalled = false;
            _standing = false;
            return;
        }

        if (sameAppend && !_standing)
            _flightSince = _clock.GetTimestamp();

        _standing = sameAppend;
        _visible = failedAgain || progress.Refused;
        if (!_visible || _stalled)
            return;

        _since = _clock.GetTimestamp();
        _stalled = true;
    }

    /// <summary>Tells whether the leader failed to append for the whole timeout.</summary>
    /// <param name="timeout">The length of the windows.</param>
    /// <returns>
    /// <see langword="true" /> when a failure window elapsed while a new failure or a refusal is still visible in the last observation, or when one
    /// append stood still in flight for the whole timeout.
    /// </returns>
    internal bool IsOverdue(TimeSpan timeout) =>
        (_stalled && _visible && _clock.GetElapsedTime(_since) >= timeout) || (_standing && _clock.GetElapsedTime(_flightSince) >= timeout);

    /// <summary>Ends the windows.</summary>
    /// <param name="timeout">The length of the windows.</param>
    /// <returns>How long the append that stood still in flight waited when it is the reason, otherwise how long the appends kept failing.</returns>
    internal TimeSpan End(TimeSpan timeout)
    {
        var standing = _standing ? _clock.GetElapsedTime(_flightSince) : TimeSpan.Zero;
        var waited = standing >= timeout ? standing : _clock.GetElapsedTime(_since);
        _stalled = false;
        _standing = false;
        return waited;
    }
}
