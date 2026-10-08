using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>A node clock whose wall time runs a fixed offset away from the system wall time, while its timers and timestamps stay the system ones.</summary>
/// <remarks>
/// Models a node whose wall clock is skewed: elections, heartbeats and every other wait keep real time, and only what reads
/// <see cref="GetUtcNow" />, such as expiration deadlines, sees the offset.
/// </remarks>
[Immutable]
public sealed class SkewedTimeProvider : TimeProvider
{
    /// <summary>Initializes a new instance of the <see cref="SkewedTimeProvider" /> class.</summary>
    /// <param name="offset">How far the wall time of the node runs ahead of the system wall time; negative when it runs behind.</param>
    public SkewedTimeProvider(TimeSpan offset)
    {
        Offset = offset;
    }

    /// <summary>Gets how far the wall time of the node runs ahead of the system wall time; negative when it runs behind.</summary>
    public TimeSpan Offset { get; }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => System.GetUtcNow() + Offset;
}
