using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness as unhealthy once this node detected a cluster ring mismatch with a peer.</summary>
[Immutable]
internal sealed class RingAgreementHealthCheck : IHealthCheck
{
    private readonly RingAgreement _agreement;

    internal RingAgreementHealthCheck(RingAgreement agreement)
    {
        ArgumentNullException.ThrowIfNull(agreement);
        _agreement = agreement;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_agreement.IsFenced || _agreement.FirstMismatch is not { } report)
            return Task.FromResult(HealthCheckResult.Healthy("cluster ring agrees with the peers that called or answered this node."));

        var direction = report.Direction == RingMismatchDirection.Inbound ? "inbound call" : "outbound call";
        return Task.FromResult(
            HealthCheckResult.Unhealthy(
                $"cluster ring mismatch with peer '{report.PeerNodeId}' detected on an {direction}; make the peer lists agree and restart the affected nodes."));
    }
}
