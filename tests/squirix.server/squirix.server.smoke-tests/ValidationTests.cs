using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for startup-time configuration validation.</summary>
public sealed class ValidationTests : SmokeTestBase
{
    /// <summary>Invalid node options fail during host startup through the options validation pipeline.</summary>
    [Fact]
    public async Task InvalidBackpressureOptionsFailOnStart()
    {
        var invalidBackpressure = new AdmissionOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 7,
            RejectThreshold = 6,
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<OptionsValidationException, TestNodeHost>(
            StartNodeAsync(GetNextHttpUri(), "nodeA", new SmokeNodeStartOptions { BackpressureOptions = invalidBackpressure }, DefaultCancellationToken));

        Assert.Contains("RejectThreshold", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Invalid memory pressure options fail during host startup through the options validation pipeline.</summary>
    [Fact]
    public async Task InvalidMemoryPressureOptionsFailOnStart()
    {
        var invalid = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 50,
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<OptionsValidationException, TestNodeHost>(
            StartNodeAsync(GetNextHttpUri(), "nodeA", new SmokeNodeStartOptions { MemoryPressureOptions = invalid }, DefaultCancellationToken));

        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
    }
}
