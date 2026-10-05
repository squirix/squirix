using System;
using System.Threading;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>A fake clock that signals each timer created on it with one due time, so a test advances it only once the timer it waits for is armed.</summary>
[ThreadSafe]
internal sealed class DueTimerClock : FakeTimeProvider
{
    private readonly TimeSpan _due;

    internal DueTimerClock(TimeSpan due)
        : base(DateTimeOffset.UnixEpoch)
    {
        _due = due;
    }

    internal SemaphoreSlim TimerCreated { get; } = new(0);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = base.CreateTimer(callback, state, dueTime, period);
        if (dueTime == _due)
            _ = TimerCreated.Release();

        return timer;
    }

    /// <summary>Discards the signals of timers created so far.</summary>
    internal void ForgetCreated()
    {
        while (TimerCreated.CurrentCount > 0)
            _ = TimerCreated.Wait(0, CancellationToken.None);
    }
}
