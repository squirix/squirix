using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Squirix.Transport.Wire;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Uses one monotonic alarm for independent attempt deadlines on an established connection.</summary>
internal sealed class BinaryCallAlarm : IDisposable
{
    private readonly IBinaryDeadline?[] _pending;
    private readonly Lock _gate = new();
    private readonly ITimer _timer;
    private long _armed = long.MaxValue;
    private int _disposed;

    internal BinaryCallAlarm(IBinaryDeadline?[] pending)
    {
        _pending = pending;
        _timer = TimeProvider.System.CreateTimer(static state => (state as BinaryCallAlarm)!.Wake(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            _timer.Dispose();
        }
    }

    internal static long DueAfter(TimeSpan duration) => checked(Stopwatch.GetTimestamp() + long.CreateChecked(Math.Ceiling(duration.TotalSeconds * Stopwatch.Frequency)));

    internal static CancellationToken FailureToken(long expires, Exception error, CancellationToken token) =>
        expires != 0 && error is OperationCanceledException { CancellationToken.IsCancellationRequested: true } canceled ? canceled.CancellationToken : token;

    internal static bool HasExpired(long expires) => expires != 0 && Stopwatch.GetTimestamp() >= expires;

    internal static TimedCancellation? Materialize(in BinaryCallOptions options) =>
        options.Expires == 0 ? null : new TimedCancellation(Remaining(options.Expires), TimeProvider.System, options.CancellationToken);

    internal void Watch(long expires)
    {
        if (expires >= Volatile.Read(ref _armed))
            return;
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) == 0 && expires < _armed)
                Arm(expires);
        }
    }

    private static TimeSpan Remaining(long expires)
    {
        var remaining = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), expires);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private void Arm(long expires)
    {
        Volatile.Write(ref _armed, expires);
        var delay = TimeSpan.FromMilliseconds(Math.Ceiling(Remaining(expires).TotalMilliseconds));
        _ = _timer.Change(delay, Timeout.InfiniteTimeSpan);
    }

    private void Wake()
    {
        List<(IBinaryDeadline Call, long Due)>? expired = null;
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return;
            Volatile.Write(ref _armed, long.MaxValue);
            var now = Stopwatch.GetTimestamp();
            var earliest = long.MaxValue;
            for (var i = 0; i < _pending.Length; i++)
            {
                var call = Volatile.Read(ref _pending[i]);
                var due = call?.Expires ?? 0;
                if (call == null || due <= 0 || !call.IsLive)
                    continue;
                if (due <= now)
                    (expired ??= []).Add((call, due));
                else
                    earliest = Math.Min(earliest, due);
            }

            if (earliest != long.MaxValue)
                Arm(earliest);
        }

        if (expired != null)
        {
            for (var i = 0; i < expired.Count; i++)
                expired[i].Call.Expire(expired[i].Due);
        }
    }
}
