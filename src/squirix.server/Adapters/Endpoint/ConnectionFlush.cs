using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Wire;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Bounds sequential writes on one connection with an alarm that is shared by all flushes.</summary>
internal sealed class ConnectionFlush : IDisposable
{
    private readonly TimeProvider _clock;
    private readonly CancellationTokenSource _cancellation;
    private readonly ITimer _timer;
    private readonly Lock _gate = new();
    private long _started;
    private bool _active;
    private bool _armed;

    internal ConnectionFlush(TimeProvider clock, CancellationToken closed)
    {
        _clock = clock;
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(closed);
        _timer = clock.CreateTimer(static state => (state as ConnectionFlush)!.OnAlarm(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _active = false;
            _timer.Dispose();
            _cancellation.Dispose();
        }
    }

    internal async ValueTask<FlushResult> WriteAsync(PipeWriter writer)
    {
        lock (_gate)
        {
            _started = _clock.GetTimestamp();
            _active = true;
            if (!_armed)
            {
                _armed = true;
                _ = _timer.Change(RpcAttemptTimeout.OperationDeadline, Timeout.InfiniteTimeSpan);
            }
        }

        try
        {
            return await writer.FlushAsync(_cancellation.Token).ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
                _active = false;
        }
    }

    private void OnAlarm()
    {
        lock (_gate)
        {
            if (_active)
            {
                var remaining = RpcAttemptTimeout.OperationDeadline - _clock.GetElapsedTime(_started);
                if (remaining > TimeSpan.Zero)
                {
                    _ = _timer.Change(remaining, Timeout.InfiniteTimeSpan);
                    return;
                }

                _ = _cancellation.CancelAsync();
            }

            _armed = false;
        }
    }
}
