using System;
using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Tracks persistent retention cleanup failures for readiness degradation.</summary>
internal sealed class RetentionCleanupReadiness : IRetentionCleanupReadinessStatus
{
    private readonly int _consecutiveWriteFailureThreshold;
    private readonly TimeSpan _failureWindow;
    private readonly Lock _lock = new();
    private readonly Queue<long> _recentFailures = new();
    private readonly TimeProvider _timeProvider;
    private readonly int _windowFailureThreshold;

    private int _consecutiveWriteFailures;

    /// <summary>Initializes a new instance of the <see cref="RetentionCleanupReadiness" /> class.</summary>
    /// <param name="options">Persistence options with the degradation thresholds and window.</param>
    /// <param name="timeProvider">The server clock: failures are queued by its monotonic timestamp, so a wall-clock step cannot reorder or stretch the window.</param>
    internal RetentionCleanupReadiness(PersistenceOptions options, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        _consecutiveWriteFailureThreshold = options.RetentionCleanupDegradedWrites;
        _failureWindow = TimeSpan.FromMinutes(options.RetentionCleanupDegradedWindowMinutes);
        _windowFailureThreshold = options.RetentionCleanupDegradedWindowFailures;
    }

    /// <inheritdoc />
    public int ConsecutiveWriteFailures
    {
        get
        {
            lock (_lock)
                return _consecutiveWriteFailures;
        }
    }

    /// <inheritdoc />
    public bool IsDegraded
    {
        get
        {
            lock (_lock)
            {
                // Pruned here too, so readiness recovers once the window passes without waiting for another cleanup outcome.
                PruneExpiredFailures();
                return IsDegradedCore();
            }
        }
    }

    /// <inheritdoc />
    public DateTime? LastFailureUtc
    {
        get
        {
            lock (_lock)
                return field;
        }

        private set;
    }

    /// <inheritdoc />
    public int RecentFailureCount
    {
        get
        {
            lock (_lock)
            {
                PruneExpiredFailures();
                return _recentFailures.Count;
            }
        }
    }

    /// <inheritdoc />
    public void RecordWriteOutcome(bool hadFailure)
    {
        lock (_lock)
        {
            if (hadFailure)
            {
                _consecutiveWriteFailures++;
                LastFailureUtc = _timeProvider.GetUtcNow().UtcDateTime;
                _recentFailures.Enqueue(_timeProvider.GetTimestamp());
                PruneExpiredFailures();
                return;
            }

            _consecutiveWriteFailures = 0;
            PruneExpiredFailures();
        }
    }

    private bool IsDegradedCore() => _consecutiveWriteFailures >= _consecutiveWriteFailureThreshold || _recentFailures.Count >= _windowFailureThreshold;

    private void PruneExpiredFailures()
    {
        while (_recentFailures.Count > 0 && _timeProvider.GetElapsedTime(_recentFailures.Peek()) > _failureWindow)
            _ = _recentFailures.Dequeue();
    }
}
