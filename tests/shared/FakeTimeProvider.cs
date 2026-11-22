using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Testing;

/// <summary>A UTC clock whose timers run synchronously when a test advances time.</summary>
public class FakeTimeProvider : TimeProvider
{
    private readonly Lock _gate = new();
    private readonly List<ScheduledTimer> _timers = [];
    private DateTimeOffset _now;
    private bool _dispatching;

    /// <summary>Initializes a new instance of the <see cref="FakeTimeProvider"/> class at midnight UTC on January 1, 2000.</summary>
    public FakeTimeProvider()
        : this(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="FakeTimeProvider"/> class at the given instant.</summary>
    /// <param name="start">The initial time.</param>
    public FakeTimeProvider(DateTimeOffset start)
    {
        _now = start.ToUniversalTime();
    }

    /// <inheritdoc />
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
            return _now;
    }

    /// <inheritdoc />
    public override long GetTimestamp() => GetUtcNow().UtcTicks;

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ScheduledTimer(this, callback, state);
        _ = timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Advances the clock by a nonnegative duration and fires timers that became due.</summary>
    /// <param name="delta">Elapsed virtual time.</param>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        lock (_gate)
            _now += delta;
        DispatchDueTimers();
    }

    /// <summary>Moves the clock forward to an absolute instant and fires timers that became due.</summary>
    /// <param name="value">The new time.</param>
    public void SetUtcNow(DateTimeOffset value)
    {
        lock (_gate)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, _now);
            _now = value.ToUniversalTime();
        }

        DispatchDueTimers();
    }

    private void DispatchDueTimers()
    {
        lock (_gate)
        {
            if (_dispatching)
                return;
            _dispatching = true;
        }

        // Callbacks can cancel or rearm any timer, including themselves.
        try
        {
            while (true)
            {
                ScheduledTimer? next = null;
                lock (_gate)
                {
                    for (var index = 0; index < _timers.Count; index++)
                    {
                        var timer = _timers[index];
                        if (timer.DueTicks <= _now.UtcTicks && (next == null || timer.DueTicks < next.DueTicks))
                            next = timer;
                    }

                    if (next == null)
                    {
                        _dispatching = false;
                        return;
                    }

                    _ = _timers.Remove(next);
                    if (next.Period > TimeSpan.Zero)
                    {
                        next.DueTicks += next.Period.Ticks;
                        _timers.Add(next);
                    }
                }

                next.Invoke();
            }
        }
        catch
        {
            lock (_gate)
                _dispatching = false;
            throw;
        }
    }

    private sealed class ScheduledTimer : ITimer
    {
        private readonly FakeTimeProvider _clock;
        private readonly TimerCallback _callback;
        private readonly object? _state;
        private int _disposed;

        internal ScheduledTimer(FakeTimeProvider clock, TimerCallback callback, object? state)
        {
            _clock = clock;
            _callback = callback;
            _state = state;
        }

        internal long DueTicks { get; set; }

        internal TimeSpan Period { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            ValidateTimeout(dueTime, nameof(dueTime));
            ValidateTimeout(period, nameof(period));
            lock (_clock._gate)
            {
                if (Volatile.Read(ref _disposed) != 0)
                    return false;

                _ = _clock._timers.Remove(this);
                if (dueTime > Timeout.InfiniteTimeSpan)
                {
                    DueTicks = _clock._now.UtcTicks + dueTime.Ticks;
                    Period = period;
                    _clock._timers.Add(this);
                }
            }

            _clock.DispatchDueTimers();
            return true;
        }

        public void Dispose() => Unschedule();

        public ValueTask DisposeAsync()
        {
            Unschedule();
            return ValueTask.CompletedTask;
        }

        internal void Invoke() => _callback(_state);

        private static void ValidateTimeout(TimeSpan timeout, string name)
        {
            var milliseconds = timeout.Ticks / TimeSpan.TicksPerMillisecond;
            if (milliseconds is < -1L or > uint.MaxValue - 1L)
                throw new ArgumentOutOfRangeException(name);
        }

        private void Unschedule()
        {
            lock (_clock._gate)
            {
                _ = Interlocked.Exchange(ref _disposed, 1);
                _ = _clock._timers.Remove(this);
            }
        }
    }
}
