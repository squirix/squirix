using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Builds ring agreement doubles for tests that route or forward calls.</summary>
internal static class RingAgreements
{
    /// <summary>Creates an unfenced agreement that discards its log output.</summary>
    /// <returns>A new agreement over a fixed two-node ring.</returns>
    internal static RingAgreement Create() => new(RingFingerprint.Create("cluster", ["n1", "n2"], 128), NullLogger<RingAgreement>.Instance);
}
