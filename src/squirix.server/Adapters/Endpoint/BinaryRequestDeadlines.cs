using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Schedules independent request cancellations with a single alarm on a bounded connection.</summary>
internal sealed class BinaryRequestDeadlines : IDisposable
{
    private readonly TimeProvider _clock;
    private readonly long _origin;
    private readonly CancellationToken _closed;
    private readonly Lock _gate = new();
    private readonly ITimer _alarm;
    private readonly CancellationTokenRegistration _shutdown;
    private int _disposed;
    private Lease?[] _active = new Lease?[4];
    private int _count;
    private long _armed = long.MaxValue;

    internal BinaryRequestDeadlines(TimeProvider clock, CancellationToken closed)
    {
        _clock = clock;
        _origin = clock.GetTimestamp();
        _closed = closed;
        _alarm = clock.CreateTimer(OnAlarm, this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _shutdown = closed.UnsafeRegister(OnConnectionClosed, this);
    }

    public void Dispose()
    {
        _alarm.Dispose();
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            if (_count == 0)
                _ = _shutdown.Unregister();
        }
    }

    internal Lease Start(TimeSpan timeout)
    {
        var now = _clock.GetElapsedTime(_origin).Ticks;
        var lease = new Lease(this, checked(now + timeout.Ticks));
        lock (_gate)
        {
            if (_closed.IsCancellationRequested)
            {
                lease.Cancel(false);
                return lease;
            }

            if (_active.Length == _count)
                Array.Resize(ref _active, checked(_count * 2));
            lease.Index = _count;
            _active[_count++] = lease;
            if (lease.Due < _armed)
                Arm(lease.Due);
        }

        return lease;
    }

    private static void OnConnectionClosed(object? state)
    {
        if (state is BinaryRequestDeadlines deadlines)
            deadlines.CloseRequests();
    }

    private static void OnAlarm(object? state)
    {
        if (state is BinaryRequestDeadlines clock)
            clock.Expire();
    }

    private void CloseRequests()
    {
        List<Exception>? errors = null;
        while (true)
        {
            Lease lease;
            lock (_gate)
            {
                if (_count == 0)
                    break;
                lease = _active[_count - 1]!;
                Remove(lease);
                lease.BeginCancellation();
            }

            try
            {
                lease.Cancel(false);
            }
            catch (AggregateException error)
            {
                (errors ??= []).Add(error);
            }
            finally
            {
                lease.FinishCallbacks();
            }
        }

        if (errors != null)
            throw new AggregateException(errors);
    }

    private void Arm(long due)
    {
        _armed = due;

        // System timers round delays down to milliseconds. Round up so a submillisecond remainder does not spin.
        var ticks = Math.Max(0, due - _clock.GetElapsedTime(_origin).Ticks);
        var ms = (ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;
        _ = _alarm.Change(TimeSpan.FromMilliseconds(ms), Timeout.InfiniteTimeSpan);
    }

    private void Expire()
    {
        lock (_gate)
        {
            _armed = long.MaxValue;
            var now = _clock.GetElapsedTime(_origin).Ticks;
            var earliest = long.MaxValue;
            for (var i = _count - 1; i >= 0; i--)
            {
                var lease = _active[i]!;
                if (lease.Due <= now)
                {
                    Remove(lease);
                    lease.Expire();
                }
                else
                {
                    earliest = Math.Min(earliest, lease.Due);
                }
            }

            if (earliest != long.MaxValue)
                Arm(earliest);
        }
    }

    private void Remove(Lease lease)
    {
        var last = _active[--_count]!;
        _active[lease.Index] = last;
        last.Index = lease.Index;
        _active[_count] = null;
        lease.Index = -1;
        if (Volatile.Read(ref _disposed) != 0 && _count == 0)
            _ = _shutdown.Unregister();
    }

    internal sealed class Lease : CancellationTokenSource
    {
        private readonly BinaryRequestDeadlines _owner;
        private bool _expiring;
        private bool _finished;
        private int _disposed;

        internal Lease(BinaryRequestDeadlines owner, long due)
        {
            _owner = owner;
            Due = due;
        }

        internal long Due { get; }
        internal int Index { get; set; } = -1;

        internal void BeginCancellation() => _expiring = true;

        /// <summary>Marks the token canceled and dispatches its callbacks outside the connection lock.</summary>
        internal void Expire()
        {
            BeginCancellation();
            _ = RunCallbacksAsync();
        }

        internal void FinishCallbacks()
        {
            bool release;
            lock (_owner._gate)
            {
                _finished = true;
                release = Volatile.Read(ref _disposed) != 0;
            }

            if (release)
                base.Dispose(true);
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing)
            {
                base.Dispose(false);
                return;
            }

            lock (_owner._gate)
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;
                if (Index >= 0)
                    _owner.Remove(this);
                if (_expiring && !_finished)
                    return;
            }

            base.Dispose(true);
        }

        private async Task RunCallbacksAsync()
        {
            try
            {
                await CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                FinishCallbacks();
            }
        }
    }
}
