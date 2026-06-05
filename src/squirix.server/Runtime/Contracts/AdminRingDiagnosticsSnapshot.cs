using System.Collections.Generic;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Ring diagnostics snapshot for admin REST endpoints.
/// </summary>
internal sealed class AdminRingDiagnosticsSnapshot
{
    public required string[] Members { get; init; }

    public required IReadOnlyList<AdminRingOwnerSample> OwnerLookupSamples { get; init; }

    public required int SampleSize { get; init; }

    public required int VirtualNodes { get; init; }

    public required IReadOnlyList<AdminRingNodeDistributionSnapshot> VnodeDistribution { get; init; }
}
