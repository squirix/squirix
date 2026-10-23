using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Per-node admission-control overrides for in-process test hosts.
/// Unset members keep the server defaults; the server validates the resulting options at startup.
/// </summary>
[Immutable]
public sealed class TestNodeBackpressureOptions
{
    /// <summary>Gets the maximum number of concurrently admitted requests, or <see langword="null" /> for the server default.</summary>
    public int? MaxInFlight { get; init; }

    /// <summary>Gets the in-flight count at which requests are slowed down, or <see langword="null" /> for the server default.</summary>
    public int? SlowdownThreshold { get; init; }

    internal AdmissionOptions ToServerOptions()
    {
        var defaults = new AdmissionOptions();
        return defaults with
        {
            MaxInFlight = MaxInFlight ?? defaults.MaxInFlight,
            SlowdownThreshold = SlowdownThreshold ?? defaults.SlowdownThreshold,
        };
    }
}
