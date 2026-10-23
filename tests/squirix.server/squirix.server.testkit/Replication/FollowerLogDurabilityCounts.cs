using System.Runtime.InteropServices;

namespace Squirix.Server.TestKit.Replication;

/// <summary>A point-in-time copy of the counters of <see cref="CountingFollowerLogFaultHooks" />.</summary>
/// <param name="MetaWrites">Metadata publishes.</param>
/// <param name="FrameWrites">Log frame writes.</param>
/// <param name="Flushes">Durable flushes of log bytes.</param>
[StructLayout(LayoutKind.Auto)]
public readonly record struct FollowerLogDurabilityCounts(long MetaWrites, long FrameWrites, long Flushes)
{
    /// <summary>Returns the counts accumulated after <paramref name="earlier" />.</summary>
    /// <param name="earlier">An earlier snapshot of the same counters.</param>
    /// <returns>The per-counter difference.</returns>
    public FollowerLogDurabilityCounts Since(in FollowerLogDurabilityCounts earlier) =>
        new(MetaWrites - earlier.MetaWrites, FrameWrites - earlier.FrameWrites, Flushes - earlier.Flushes);
}
