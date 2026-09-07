using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Election eligibility verdict for automatic failover.</summary>
/// <param name="Eligible">Whether the node may start an election.</param>
/// <param name="Denial">The denial reason when the node is not eligible.</param>
[Immutable]
internal readonly record struct FailoverEligibilityVerdict(bool Eligible, FailoverDenial Denial);
