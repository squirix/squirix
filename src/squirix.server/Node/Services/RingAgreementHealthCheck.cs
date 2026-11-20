using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness as unhealthy once this node detected a cluster ring mismatch with a peer.</summary>
[Immutable]
internal sealed class RingAgreementHealthCheck : IReadinessCheck
{
    private readonly RingAgreement _agreement;

    internal RingAgreementHealthCheck(RingAgreement agreement)
    {
        ArgumentNullException.ThrowIfNull(agreement);
        _agreement = agreement;
    }

    /// <inheritdoc />
    public Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken)
    {
        if (!_agreement.IsFenced || _agreement.FirstMismatch is not { } report)
            return Task.FromResult(ReadinessResult.Healthy("cluster ring agrees with the peers that called or answered this node."));

        var direction = report.Direction == RingMismatchDirection.Inbound ? "inbound call" : "outbound call";
        return Task.FromResult(
            ReadinessResult.Unhealthy(
                $"cluster ring mismatch with peer '{report.PeerNodeId}' detected on an {direction}; make the peer lists agree and restart the affected nodes."));
    }
}
