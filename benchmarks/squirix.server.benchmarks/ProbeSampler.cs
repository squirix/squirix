using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Benchmarks;

/// <summary>Polls a <see cref="JournalStallProbe" /> the way the readiness health check reads it and keeps the longest journal I/O call seen in progress.</summary>
[Mutable]
internal sealed class ProbeSampler
{
    private readonly JournalStallProbe _probe;
    private long _maxAgeTicks;
    private ITimer? _timer;

    /// <summary>Initializes a new instance of the <see cref="ProbeSampler" /> class.</summary>
    /// <param name="probe">The probe to poll.</param>
    internal ProbeSampler(JournalStallProbe probe)
    {
        ArgumentNullException.ThrowIfNull(probe);
        _probe = probe;
    }

    /// <summary>Gets the longest age of an in-progress journal I/O call observed so far.</summary>
    internal TimeSpan MaxAge => TimeSpan.FromTicks(Interlocked.Read(ref _maxAgeTicks));

    /// <summary>Starts polling about every millisecond.</summary>
    internal void Start() => _timer = TimeProvider.System.CreateTimer(_ => Poll(), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(1));

    /// <summary>Stops polling.</summary>
    internal void Stop() => _timer?.Dispose();

    private void Poll()
    {
        if (!_probe.TryReadIo(out _, out var started))
            return;

        var age = TimeProvider.System.GetElapsedTime(started).Ticks;
        if (age > Interlocked.Read(ref _maxAgeTicks))
            _ = Interlocked.Exchange(ref _maxAgeTicks, age);
    }
}
