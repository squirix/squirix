using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.SmokeTests.Config;

/// <summary>
/// Smoke tests for startup-time configuration validation.
/// </summary>
public sealed class ConfigValidationTests : SmokeTestBase
{
    /// <summary>
    /// Invalid node options fail during host startup through the options validation pipeline.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task InvalidBackpressureOptionsFailOnStart()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };
        var invalidBackpressure = new BackpressureOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 7,
            RejectThreshold = 6,
        };

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(async () => await StartNodeAsync(
            url,
            peers,
            backpressureOptions: invalidBackpressure,
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken));

        Assert.Contains("RejectThreshold", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Invalid memory pressure options fail during host startup through the options validation pipeline.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task InvalidMemoryPressureOptionsFailOnStart()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };
        var invalid = new MemoryPressureOptions
        {
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 50,
        };

        var ex = await Assert.ThrowsAsync<OptionsValidationException>(async () => await StartNodeAsync(
            url,
            peers,
            memoryPressureOptions: invalid,
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken));

        Assert.Contains("HighPressureThresholdPercent", ex.Message, StringComparison.Ordinal);
    }
}
