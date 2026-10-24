using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The sustained rate of new mutations the node-wide replay store admits, driven on a virtual clock.</summary>
[Immutable]
public sealed class IdempotencyAdmissionRateTests : DisposableServerUnitTestBase
{
    private const double BelowCapacityFactor = 0.95;
    private const double AboveCapacityFactor = 1.05;
    private const int ObservedRetentionPeriods = 3;

    private static readonly byte[] ResponseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });

    private readonly Meter _testMeter = new("test");

    /// <summary>A rate just below the capacity of the store over the minimum retention is admitted with no refusal while the store stays full.</summary>
    [Test]
    public async Task RateBelowCapacityIsNeverRefused()
    {
        var options = new IdempotencyOptions();

        var refusals = DriveAdmissions(options, BelowCapacityFactor);

        _ = await Assert.That(refusals).IsEqualTo(0);
    }

    /// <summary>A rate just above the capacity of the store over the minimum retention is refused retryably once the store is full.</summary>
    [Test]
    public async Task RateAboveCapacityIsRefused()
    {
        var options = new IdempotencyOptions();

        var refusals = DriveAdmissions(options, AboveCapacityFactor);

        _ = await Assert.That(refusals).IsGreaterThan(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static int OperationsPerSecond(IdempotencyOptions options, double factor) => Convert.ToInt32(Math.Floor(factor * options.MaxInFlightRecords / options.MinRetention.TotalSeconds), CultureInfo.InvariantCulture);

    /// <summary>Admits new operations at a fixed rate per virtual second from an empty store for several minimum retentions.</summary>
    /// <param name="options">The store options.</param>
    /// <param name="factor">The rate as a multiple of the capacity over the minimum retention.</param>
    /// <returns>The number of refused operations.</returns>
    private int DriveAdmissions(IdempotencyOptions options, double factor)
    {
        var clock = new FakeTimeProvider();
        var store = new RpcMutationIdempotencyStore(options, "test-node", new IdempotencyMetrics(_testMeter), clock);
        var rate = OperationsPerSecond(options, factor);
        var seconds = Convert.ToInt32(ObservedRetentionPeriods * options.MinRetention.TotalSeconds, CultureInfo.InvariantCulture);
        var refusals = 0;
        var next = 0;
        for (var second = 0; second < seconds; second++)
        {
            for (var i = 0; i < rate; i++)
            {
                var operationId = string.Create(CultureInfo.InvariantCulture, $"op-{next++}");
                try
                {
                    _ = store.ReserveIntent(operationId, "fp", null, out _);
                    store.RecordSuccess(operationId, "fp", ResponseBytes, null);
                }
                catch (SquirixException)
                {
                    refusals++;
                }
            }

            clock.Advance(TimeSpan.FromSeconds(1));
        }

        return refusals;
    }
}
