using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Deterministic one-shot election timeout driven by an injected <see cref="TimeProvider" />.</summary>
/// <remarks>
/// A single-node group never elects: <see cref="Create" /> returns <see langword="null" /> for a replica factor
/// of one, so RF=1 hosts carry no election timer. Multi-node groups arm the timer while awaiting a leader
/// heartbeat and re-arm it on every valid heartbeat via <see cref="Reset" />.
/// </remarks>
[ThreadSafe]
[SuppressMessage(
    "Usage",
    "MA0182:Internal type is apparently never used",
    Justification = "Test-only activation seam until failover activation wires election timers in a follow-up milestone.")]
internal sealed class ElectionTimer : IDisposable
{
    private readonly Lock _sync = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _timeout;

    private int _disposed;
    private Action? _elapsed;
    private ITimer? _timer;

    /// <summary>Initializes a new instance of the <see cref="ElectionTimer" /> class.</summary>
    /// <param name="timeout">The one-shot election timeout; must be positive.</param>
    /// <param name="timeProvider">The time source driving the timer; <see langword="null" /> selects <see cref="TimeProvider.System" />.</param>
    private ElectionTimer(TimeSpan timeout, TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        _timeout = timeout;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        ITimer? timer;
        lock (_sync)
        {
            timer = _timer;
            _timer = null;
            _elapsed = null;
        }

        timer?.Dispose();
    }

    /// <summary>Creates an election timer for a multi-node group, or <see langword="null" /> for RF=1.</summary>
    /// <param name="count">The configured replica factor, including the leader.</param>
    /// <param name="options">The timer configuration; <see langword="null" /> selects the defaults.</param>
    /// <param name="timeProvider">The time source driving the timer; <see langword="null" /> selects <see cref="TimeProvider.System" />.</param>
    /// <returns>An election timer for multi-node groups; otherwise <see langword="null" />.</returns>
    internal static ElectionTimer? Create(int count, ElectionTimerOptions? options = null, TimeProvider? timeProvider = null)
    {
        var electionTimerOptions = options ?? new ElectionTimerOptions();
        return count <= 1 ? null : new ElectionTimer(electionTimerOptions.ElectionTimeout, timeProvider);
    }

    /// <summary>Re-arms the one-shot timeout; a no-op before <see cref="Start" />.</summary>
    internal void Reset()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        lock (_sync)
            _ = _timer?.Change(_timeout, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Arms the one-shot timeout invoking <paramref name="elapsed" /> once on expiry.</summary>
    /// <param name="elapsed">The callback invoked once when the timeout elapses.</param>
    internal void Start(Action elapsed)
    {
        ArgumentNullException.ThrowIfNull(elapsed);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            _elapsed = elapsed;
            _timer ??= _timeProvider.CreateTimer(OnTick, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            _ = _timer.Change(_timeout, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Invokes the armed callback outside the gate so re-arming from the callback cannot deadlock.</summary>
    /// <param name="state">Unused timer state.</param>
    private void OnTick(object? state)
    {
        Action? elapsed;
        lock (_sync)
            elapsed = _elapsed;

        elapsed?.Invoke();
    }
}
