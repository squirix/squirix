using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for <see cref="JournalRecoveryReadinessHealthCheck" />.</summary>
[Immutable]
public sealed class JournalRecoveryReadinessHealthCheckTests
{
    /// <summary>Rejects a null readiness gate.</summary>
    [Test]
    public void RejectsNullGate()
    {
        AsyncManualResetEvent? gate = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(gate, static value => _ = new JournalRecoveryReadinessHealthCheck(value!));
    }

    /// <summary>Reports healthy once journal recovery signals completion.</summary>
    [Test]
    public async Task ReportsHealthyAfterRecoveryAsync()
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();

        var result = await new JournalRecoveryReadinessHealthCheck(gate).CheckHealthAsync(CancellationToken.None);

        _ = await Assert.That(result.Status).IsEqualTo(ReadinessStatus.Healthy);
    }

    /// <summary>Reports unhealthy while journal recovery is still in progress.</summary>
    [Test]
    public async Task ReportsUnhealthyWhilePendingAsync()
    {
        var result = await new JournalRecoveryReadinessHealthCheck(new AsyncManualResetEvent()).CheckHealthAsync(CancellationToken.None);

        _ = await Assert.That(result.Status).IsEqualTo(ReadinessStatus.Unhealthy);
    }
}
