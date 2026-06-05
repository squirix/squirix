using System;
using Squirix.Server.Node.Backpressure;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Backpressure;

/// <summary>
/// Unit tests covering validation and defaults for <see cref="BackpressureOptions" />.
/// </summary>
public sealed class BackpressureOptionsTests
{
    /// <summary>
    /// Ensures the default configuration passes validation and exposes conservative defaults.
    /// </summary>
    [Fact]
    public void DefaultsAreValid()
    {
        var options = new BackpressureOptions();

        var ex = Record.Exception(options.Validate);

        Assert.Null(ex);
        Assert.True(options.Enabled);
        Assert.Equal(256, options.MaxInFlight);
        Assert.Equal(128, options.MaxQueue);
        Assert.Null(options.PerClientMaxInFlight);
        Assert.Null(options.NodeRateLimitPerSecond);
    }

    /// <summary>
    /// Ensures rate limiting requires both refill rate and burst capacity.
    /// </summary>
    [Fact]
    public void ValidateThrowsForIncompleteRateLimit()
    {
        var options = new BackpressureOptions
        {
            NodeRateLimitPerSecond = 100,
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains("NodeRateLimitBurst", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures per-client concurrency cannot be configured above the global node cap.
    /// </summary>
    [Fact]
    public void ValidateThrowsForInvalidPerClientConcurrency()
    {
        var options = new BackpressureOptions
        {
            MaxInFlight = 8,
            PerClientMaxInFlight = 9,
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains("PerClientMaxInFlight", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures invalid threshold ordering is rejected during validation.
    /// </summary>
    [Fact]
    public void ValidateThrowsForInvalidThresholdOrdering()
    {
        var options = new BackpressureOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 6,
            RejectThreshold = 5,
        };

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains("RejectThreshold", ex.Message, StringComparison.Ordinal);
    }
}
