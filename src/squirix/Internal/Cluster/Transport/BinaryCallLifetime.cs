using System.Threading;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Arbitrates reply, cancellation and timeout when a correlation slot is reused.</summary>
internal struct BinaryCallLifetime
{
    private const long Completed = -1;
    private long _expires;

    internal BinaryCallLifetime(long expires)
    {
        _expires = expires;
    }

    internal readonly long Expires => Volatile.Read(in _expires);

    internal readonly bool IsCompleted => Expires == Completed;

    internal void Start(long expires) => Volatile.Write(ref _expires, expires);

    internal bool TryComplete() => Interlocked.Exchange(ref _expires, Completed) != Completed;

    /// <summary>Claims an expired call only if its originally observed deadline still matches.</summary>
    /// <param name="observed">The monotonic timestamp captured by the alarm.</param>
    /// <returns>True if this sweep claimed the call.</returns>
    internal bool TryExpire(long observed) =>
        observed > 0 && Interlocked.CompareExchange(ref _expires, Completed, observed) == observed;
}
