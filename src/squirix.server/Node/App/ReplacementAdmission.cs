using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.App;

/// <summary>Checks the memory growth of a conditional replacement against the entry selected under its mutation gate.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[ThreadSafe]
internal sealed class ReplacementAdmission<T>
{
    private readonly PressureGate _gate;
    private readonly ICacheEntrySizeEstimator<T> _estimator;
    private long _pendingJournalGrowth;

    internal ReplacementAdmission(PressureGate gate, ICacheEntrySizeEstimator<T> estimator)
    {
        _gate = gate;
        _estimator = estimator;
    }

    internal void Check(CacheKey key, NodeCacheEntry<T> current, NodeCacheEntry<T> replacement, Func<bool>? hasMemberPressure = null)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, current, replacement, _estimator, out var magnitudeUnknown);
        _gate.ThrowIfMemoryGrowingWriteRejected(growth, magnitudeUnknown, AdmissionOperations.Set);
        if ((magnitudeUnknown || growth > 0) && hasMemberPressure?.Invoke() == true)
            _gate.ThrowReplicaPressureRejection(AdmissionOperations.Set);
    }

    internal long ReserveJournalGrowth(CacheKey key, NodeCacheEntry<T> current, NodeCacheEntry<T> replacement)
    {
        var growth = MemoryAdmissionJournalExtensions.ComputeNetGrowthForReplace(key, current, replacement, _estimator, out var magnitudeUnknown);
        if (growth == 0 && !magnitudeUnknown)
            return 0;

        // The append gate serializes reservations. Apply or failure releases them.
        var pending = Interlocked.Read(ref _pendingJournalGrowth);
        var combined = Math.Min(pending, long.MaxValue - growth) + growth;
        _gate.ThrowIfMemoryGrowingWriteRejected(combined, magnitudeUnknown, AdmissionOperations.Set);
        _ = Interlocked.Add(ref _pendingJournalGrowth, growth);
        return growth;
    }

    internal void ReleaseJournalGrowth(long growth) => _ = Interlocked.Add(ref _pendingJournalGrowth, -growth);
}
