using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A fake clock that counts the timers created on it. A loop that waits with a timeout on this clock creates one timer each time it parks,
/// so the count tells a test that the passes before it have ended.
/// </summary>
internal sealed class ParkingClock : TimeProvider
{
    private readonly FakeTimeProvider _inner = new();
    private readonly Lock _sync = new();
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];
    private int _timers;

    /// <inheritdoc />
    public override long TimestampFrequency => _inner.TimestampFrequency;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => _inner.GetUtcNow();

    /// <inheritdoc />
    public override long GetTimestamp() => _inner.GetTimestamp();

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = _inner.CreateTimer(callback, state, dueTime, period);
        lock (_sync)
        {
            _timers++;
            foreach (var (count, reached) in _waiters)
            {
                if (_timers >= count)
                    _ = reached.TrySetResult();
            }
        }

        return timer;
    }

    /// <summary>Returns a task that completes once at least <paramref name="count" /> timers were created on the clock.</summary>
    /// <param name="count">The number of timers to wait for.</param>
    /// <returns>The task.</returns>
    internal Task WhenTimersAsync(int count)
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_sync)
        {
            if (_timers >= count)
                _ = reached.TrySetResult();
            else
                _waiters.Add((count, reached));
        }

        return reached.Task;
    }
}
