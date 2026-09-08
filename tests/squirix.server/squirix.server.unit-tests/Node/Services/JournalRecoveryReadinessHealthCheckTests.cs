using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Unit tests for <see cref="JournalRecoveryReadinessHealthCheck" />.
/// </summary>
[Immutable]
public sealed class JournalRecoveryReadinessHealthCheckTests
{
    /// <summary>Reports healthy once journal recovery signals completion.</summary>
    [Fact]
    public async Task ReportsHealthyAfterRecoveryAsync()
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();

        var result = await new JournalRecoveryReadinessHealthCheck(gate).CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>Reports unhealthy while journal recovery is still in progress.</summary>
    [Fact]
    public async Task ReportsUnhealthyWhilePendingAsync()
    {
        var result = await new JournalRecoveryReadinessHealthCheck(new AsyncManualResetEvent()).CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    /// <summary>Rejects a null readiness gate.</summary>
    [Fact]
    public void RejectsNullGate()
    {
        AsyncManualResetEvent? gate = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(gate, static value => _ = new JournalRecoveryReadinessHealthCheck(value!));
    }
}
