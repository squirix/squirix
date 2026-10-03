using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for <see cref="RingAgreementHealthCheck" />.</summary>
[Immutable]
public sealed class RingAgreementHealthCheckTests
{
    /// <summary>Rejects a null agreement.</summary>
    [Test]
    public void RejectsNullAgreement()
    {
        RingAgreement? agreement = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(agreement, static value => _ = new RingAgreementHealthCheck(value!));
    }

    /// <summary>Reports healthy while no mismatch was detected.</summary>
    [Test]
    public async Task ReportsHealthyWithoutMismatch()
    {
        var result = await new RingAgreementHealthCheck(CreateAgreement()).CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Reports unhealthy and names the peer and direction once fenced.</summary>
    [Test]
    public async Task ReportsUnhealthyNamingPeerWhenFenced()
    {
        var agreement = CreateAgreement();
        agreement.ReportOutboundMismatch("n2");

        var result = await new RingAgreementHealthCheck(agreement).CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("'n2'", StringComparison.Ordinal);
        _ = await Assert.That(result.Description).Contains("outbound", StringComparison.Ordinal);
    }

    private static RingAgreement CreateAgreement() => new(RingFingerprint.Create("cluster", ["n1", "n2"], 128), new EventRecordingLogger());
}
