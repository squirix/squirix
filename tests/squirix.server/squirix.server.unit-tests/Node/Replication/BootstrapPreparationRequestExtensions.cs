using Squirix.Server.Cluster;
using Squirix.Server.Node.Replication;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Test-only builders for bootstrap preparation requests.</summary>
internal static class BootstrapPreparationRequestExtensions
{
    /// <summary>Returns a copy of the request with a replaced target topology.</summary>
    /// <param name="request">Source request.</param>
    /// <param name="target">Replacement target topology.</param>
    /// <returns>A request identical to the source except for the target topology.</returns>
    internal static BootstrapPreparationRequest WithTarget(this BootstrapPreparationRequest request, TopologyOptions target)
    {
        return new BootstrapPreparationRequest
        {
            GroupIds = request.GroupIds,
            LegacyOutcomes = request.LegacyOutcomes,
            Persistence = request.Persistence,
            SourceMtls = request.SourceMtls,
            SourceTopology = request.SourceTopology,
            TargetMtls = request.TargetMtls,
            TargetTopology = target,
        };
    }
}
