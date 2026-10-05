using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>White-box coverage for <see cref="ClientPool" /> bootstrap warm-up with a fail-fast connect budget.</summary>
[Immutable]
public sealed class ClientPoolWarmUpTests
{
    private static readonly BootstrapConnectOptions FailFastConnectOptions = new(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200));

    /// <summary>Verifies warm-up fails when no bootstrap endpoint can be reached.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WarmUpFailsFastOnUnreachableEndpoint(CancellationToken cancellationToken)
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "peer-0",
                Uri = new Uri("https://127.0.0.1:1"),
            },
        };

        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var pool = new ClientPool(peers, static _ => new CallPolicy(), connectOptions: FailFastConnectOptions, timeProvider: clock);

        // Every wait of the warm-up (per-attempt timeout, backoff, overall deadline) runs on the fake clock, so advancing it drives the warm-up to its outcome.
        var warmUp = pool.WarmUpAsync(cancellationToken).AsTask();
        while (!warmUp.IsCompleted)
        {
            clock.Advance(FailFastConnectOptions.OverallDeadline);
            await Task.Yield();
        }

        var exception = await AsyncAssert.ThrowsAsync<InvalidOperationException, string>(new ValueTask<string>(warmUp));
        _ = await Assert.That(exception.Message).Contains("Failed to connect to endpoint", StringComparison.Ordinal);
    }
}
