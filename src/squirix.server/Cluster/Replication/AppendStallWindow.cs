using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The window in which the appends of a leader to its own group log fail or hang without a success, as the election driver observes it tick by tick.</summary>
/// <remarks>
/// The window starts at the first tick that sees a new failure or a blocked append after the last success, and any success clears it. The
/// first observation of a leadership only records the counters, so failures before it never count. Not thread safe: the election driver of
/// the group is its only caller.
/// </remarks>
internal sealed class AppendStallWindow
{
    private readonly TimeProvider _clock;
    private ulong _appended;
    private ulong _failed;
    private bool _seeded;
    private long _since;
    private bool _stalled;
    private bool _visible;

    /// <summary>Initializes a new instance of the <see cref="AppendStallWindow" /> class.</summary>
    /// <param name="clock">The clock of the group.</param>
    internal AppendStallWindow(TimeProvider clock)
    {
        _clock = clock;
    }

    /// <summary>Forgets the counters and the window, so the next observation only records the counters.</summary>
    internal void Reset()
    {
        _seeded = false;
        _stalled = false;
        _visible = false;
    }

    /// <summary>Records what the appends did since the previous observation.</summary>
    /// <param name="progress">The progress of the local appends.</param>
    internal void Observe(in ReplicaAppendProgress progress)
    {
        var seeded = _seeded;
        var advanced = progress.Appended != _appended;
        var failedAgain = progress.Failed != _failed;
        _appended = progress.Appended;
        _failed = progress.Failed;
        _seeded = true;
        _visible = false;
        if (!seeded || advanced)
        {
            _stalled = false;
            return;
        }

        _visible = failedAgain || progress.Blocked;
        if (!_visible || _stalled)
            return;

        _since = _clock.GetTimestamp();
        _stalled = true;
    }

    /// <summary>Tells whether the window elapsed while the stall is still visible in the last observation.</summary>
    /// <param name="timeout">The length of the window.</param>
    /// <returns><see langword="true" /> when the window runs and elapsed, and the last observation saw a new failure or a blocked append.</returns>
    internal bool IsOverdue(TimeSpan timeout) => _stalled && _visible && _clock.GetElapsedTime(_since) >= timeout;

    /// <summary>Ends the window.</summary>
    /// <returns>How long the appends kept failing.</returns>
    internal TimeSpan End()
    {
        var waited = _clock.GetElapsedTime(_since);
        _stalled = false;
        return waited;
    }
}
