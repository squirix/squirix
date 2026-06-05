using System;
using Squirix.Server.Timing;

namespace Squirix.Server.TestKit.Utils;

/// <summary>
/// Test clock with manual time advancement for server runtime tests.
/// </summary>
public sealed class FakeClock : IClock
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FakeClock" /> class.
    /// </summary>
    /// <param name="startUtc">Optional starting UTC time.</param>
    public FakeClock(DateTime? startUtc = null)
    {
        UtcNow = (startUtc ?? DateTime.UtcNow).ToUniversalTime();
    }

    /// <inheritdoc />
    public DateTime UtcNow { get; private set; }

    /// <summary>
    /// Advances the clock by the specified delta.
    /// </summary>
    /// <param name="delta">The amount of time to advance.</param>
    public void Advance(TimeSpan delta) => UtcNow = UtcNow.Add(delta);
}
