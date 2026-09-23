using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for startup-time configuration validation.</summary>
public sealed class ValidationTests : SmokeTestBase
{
    /// <summary>Invalid node options fail during host startup through the options validation pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidBackpressureOptionsFailOnStart(CancellationToken cancellationToken)
    {
        var invalidBackpressure = new AdmissionOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 7,
            RejectThreshold = 6,
        };

        var operation = StartClusterAsync("nodeA", _ => new SmokeStartOptions { BackpressureOptions = invalidBackpressure }, cancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<OptionsValidationException, TestCluster<SmokeStartOptions>>(operation);

        _ = await Assert.That(ex.Message).Contains("RejectThreshold", StringComparison.Ordinal);
    }

    /// <summary>Invalid memory pressure options fail during host startup through the options validation pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidMemoryPressureOptionsFailOnStart(CancellationToken cancellationToken)
    {
        var invalid = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 50,
        };

        var operation = StartClusterAsync("nodeA", _ => new SmokeStartOptions { MemoryPressureOptions = invalid }, cancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<OptionsValidationException, TestCluster<SmokeStartOptions>>(operation);

        _ = await Assert.That(ex.Message).Contains("HighPressureThresholdPercent", StringComparison.Ordinal);
    }
}
