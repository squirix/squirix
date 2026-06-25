using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.SmokeTests.Support;
using Xunit;

namespace Squirix.Server.SmokeTests.Config;

/// <summary>Smoke tests for startup-time configuration validation.</summary>
public sealed class ConfigValidationTests : SmokeTestBase
{
    /// <summary>Invalid node options fail during host startup through the options validation pipeline.</summary>
    [Fact]
    public async Task InvalidBackpressureOptionsFailOnStart()
    {
        var invalidBackpressure = new BackpressureOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 7,
            RejectThreshold = 6,
        };

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            StartNodeAsync(
                GetNextHttpUri(),
                "nodeA",
                backpressureOptions: invalidBackpressure,
                cancellationToken: DefaultCancellationToken).AsTask());

        Assert.Contains("RejectThreshold", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Invalid memory pressure options fail during host startup through the options validation pipeline.</summary>
    [Fact]
    public async Task InvalidMemoryPressureOptionsFailOnStart()
    {
        var invalid = new MemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 50,
        };

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            StartNodeAsync(
                GetNextHttpUri(),
                "nodeA",
                memoryPressureOptions: invalid,
                cancellationToken: DefaultCancellationToken).AsTask());

        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
    }
}
