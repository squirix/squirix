using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests covering validation and defaults for <see cref="AdmissionOptions" />.</summary>
[Immutable]
public sealed class BackpressureOptionsTests
{
    /// <summary>Ensures the default configuration passes validation and exposes conservative defaults.</summary>
    [Test]
    public async Task DefaultsAreValid()
    {
        var options = new AdmissionOptions();

        options.Validate();
        _ = await Assert.That(options.Enabled).IsTrue();
        _ = await Assert.That(options.MaxInFlight).IsEqualTo(256);
        _ = await Assert.That(options.MaxQueue).IsEqualTo(128);
        _ = await Assert.That(options.PerClientMaxInFlight).IsNull();
        _ = await Assert.That(options.NodeRateLimitPerSecond).IsNull();
    }

    /// <summary>Ensures per-client concurrency cannot be configured above the global node cap.</summary>
    [Test]
    public async Task ThrowsForInvalidPerClientConcurrency()
    {
        var options = new AdmissionOptions
        {
            MaxInFlight = 8,
            PerClientMaxInFlight = 9,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains("PerClientMaxInFlight", StringComparison.Ordinal);
    }

    /// <summary>Ensures invalid threshold ordering is rejected during validation.</summary>
    [Test]
    public async Task ThrowsForInvalidThresholdOrdering()
    {
        var options = new AdmissionOptions
        {
            MaxInFlight = 8,
            SlowdownThreshold = 9,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains("SlowdownThreshold", StringComparison.Ordinal);
    }

    /// <summary>Ensures a queue wait above the operational cap is rejected.</summary>
    [Test]
    public async Task ThrowsForExcessiveQueueWait()
    {
        var options = new AdmissionOptions { MaxQueueWait = TimeSpan.FromMinutes(1) + TimeSpan.FromMilliseconds(1) };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains("MaxQueueWait cannot exceed 00:01:00", StringComparison.Ordinal);
    }

    /// <summary>Ensures a slowdown delay above the operational cap is rejected.</summary>
    [Test]
    public async Task ThrowsForExcessiveSlowdownDelay()
    {
        var options = new AdmissionOptions { MaxSlowdownDelay = TimeSpan.FromSeconds(5) + TimeSpan.FromMilliseconds(1) };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains("MaxSlowdownDelay cannot exceed 00:00:05", StringComparison.Ordinal);
    }

    /// <summary>Ensures the operational caps themselves are accepted.</summary>
    [Test]
    public void ValidateAcceptsDelayCaps()
    {
        var options = new AdmissionOptions { MaxQueueWait = AdmissionOptions.MaxQueueWaitLimit, MaxSlowdownDelay = AdmissionOptions.MaxSlowdownDelayLimit };

        options.Validate();
    }

    /// <summary>Ensures a burst without a rate names the missing rate.</summary>
    [Test]
    public async Task ThrowsForBurstWithoutRate()
    {
        var options = new AdmissionOptions { PerClientRateLimitBurst = 5 };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).IsEqualTo("Backpressure PerClientRateLimitBurst requires PerClientRateLimitPerSecond.");
    }

    /// <summary>Ensures rate limiting requires both refill rate and burst capacity.</summary>
    [Test]
    public async Task ValidateThrowsForIncompleteRateLimit()
    {
        var options = new AdmissionOptions
        {
            NodeRateLimitPerSecond = 100,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains("NodeRateLimitBurst", StringComparison.Ordinal);
    }
}
