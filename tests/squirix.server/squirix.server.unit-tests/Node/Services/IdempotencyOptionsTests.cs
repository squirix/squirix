using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Idempotency options reject a background sweep interval the timer cannot run.</summary>
[Immutable]
public sealed class IdempotencyOptionsTests
{
    private static readonly TimeSpan LongestTimerPeriod = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>A sweep interval above the longest timer period fails validation with a message naming the setting and the limit.</summary>
    [Test]
    public async Task SweepIntervalAboveTimerLimitFails()
    {
        var options = new IdempotencyOptions { BackgroundSweepInterval = LongestTimerPeriod + TimeSpan.FromMilliseconds(1) };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static o => o.Validate());

        _ = await Assert.That(ex.Message).Contains("BackgroundSweepInterval");
        _ = await Assert.That(ex.Message).Contains(LongestTimerPeriod.ToString("c"));
    }

    /// <summary>The longest timer period passes validation and the sweep service starts a timer with it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepServiceStartsAtLongestInterval(CancellationToken cancellationToken)
    {
        var options = new IdempotencyOptions { BackgroundSweepInterval = LongestTimerPeriod };
        options.Validate();
        using var meter = new System.Diagnostics.Metrics.Meter("test");
        var clock = new FakeTimeProvider();
        var store = new RpcMutationIdempotencyStore(options, "local", new IdempotencyMetrics(meter), clock);
        using var sweep = new IdempotencyStoreSweepService(store, Options.Create(options), NullLogger<IdempotencyStoreSweepService>.Instance, clock);

        await sweep.StartAsync(cancellationToken);
        await sweep.StopAsync(cancellationToken);

        _ = await Assert.That(sweep.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
    }
}
