using System;
using System.Threading;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>A fake clock whose wall time can step while its monotonic timestamp moves forward only.</summary>
[ThreadSafe]
internal sealed class WallSteppedClock : FakeTimeProvider
{
    private long _wallOffsetTicks;

    internal WallSteppedClock()
        : base(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
    {
    }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => base.GetUtcNow().AddTicks(Interlocked.Read(ref _wallOffsetTicks));

    /// <summary>Reads the unstepped time, since the base derives timestamps from the virtual wall time; the monotonic clock never moves back.</summary>
    /// <returns>The monotonic timestamp.</returns>
    public override long GetTimestamp() => base.GetUtcNow().UtcTicks;

    /// <summary>Moves the wall time without moving the monotonic timestamp.</summary>
    /// <param name="step">The step; negative steps the wall clock back.</param>
    internal void StepWallClock(TimeSpan step) => _ = Interlocked.Add(ref _wallOffsetTicks, step.Ticks);
}
