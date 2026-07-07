using System;
using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Storage;

/// <summary>Tracks persistent retention cleanup failures for readiness degradation.</summary>
internal sealed class RetentionCleanupReadiness : IRetentionCleanupReadinessStatus
{
    private readonly int _consecutiveWriteFailureThreshold;
    private readonly TimeSpan _failureWindow;
    private readonly Lock _lock = new();
    private readonly Queue<DateTime> _recentFailures = new();
    private readonly int _windowFailureThreshold;

    private int _consecutiveWriteFailures;
    private DateTime? _lastFailureUtc;

    public RetentionCleanupReadiness(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _consecutiveWriteFailureThreshold = options.RetentionCleanupDegradedConsecutiveWrites;
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
                return IsDegradedCore();
        }
    }

    /// <inheritdoc />
    public DateTime? LastFailureUtc
    {
        get
        {
            lock (_lock)
                return _lastFailureUtc;
        }
    }

    /// <inheritdoc />
    public int RecentFailureCount
    {
        get
        {
            lock (_lock)
            {
                PruneExpiredFailures(DateTime.UtcNow);
                return _recentFailures.Count;
            }
        }
    }

    /// <inheritdoc />
    public void RecordWriteOutcome(bool hadFailure)
    {
        var utcNow = DateTime.UtcNow;
        lock (_lock)
        {
            if (hadFailure)
            {
                _consecutiveWriteFailures++;
                _lastFailureUtc = utcNow;
                _recentFailures.Enqueue(utcNow);
                PruneExpiredFailures(utcNow);
                return;
            }

            _consecutiveWriteFailures = 0;
            PruneExpiredFailures(utcNow);
        }
    }

    private bool IsDegradedCore() => _consecutiveWriteFailures >= _consecutiveWriteFailureThreshold || _recentFailures.Count >= _windowFailureThreshold;

    private void PruneExpiredFailures(DateTime utcNow)
    {
        var cutoff = utcNow - _failureWindow;
        while (_recentFailures.Count > 0 && _recentFailures.Peek() < cutoff)
            _ = _recentFailures.Dequeue();
    }
}
