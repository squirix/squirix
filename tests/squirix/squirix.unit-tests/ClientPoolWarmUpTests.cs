using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Testing;
using Squirix.TestKit;
using Squirix.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>White-box coverage for <see cref="ClientPool" /> bootstrap warm-up on a fake clock.</summary>
[Immutable]
public sealed class ClientPoolWarmUpTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Budgets that would never elapse in real time, so only the fake clock can end the warm-up.</summary>
    private static readonly BootstrapConnectOptions FakeOnlyConnectOptions = new(TimeSpan.FromHours(1), TimeSpan.FromHours(4));

    /// <summary>Verifies warm-up fails when no bootstrap endpoint can be reached.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WarmUpFailsFastOnUnreachableEndpoint(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var pool = CreatePool(new Uri("https://127.0.0.1:1"), clock);

        // Every wait of the warm-up (per-attempt timeout, backoff, overall deadline) runs on the fake clock, so advancing it drives the warm-up to its outcome.
        var warmUp = pool.WarmUpAsync(cancellationToken).AsTask();
        await AdvanceUntilCompletedAsync(clock, warmUp);

        try
        {
            var exception = await AsyncAssert.ThrowsAsync<InvalidOperationException, string>(new ValueTask<string>(warmUp), Bound, TimeProvider.System, cancellationToken);
            _ = await Assert.That(exception.Message).Contains("Failed to connect to endpoint", StringComparison.Ordinal);
        }
        catch (SquirixTransportException exception)
        {
            // The socket can report that the endpoint is unavailable before the fake deadline cancels the attempt.
            _ = await Assert.That(exception.StatusCode).IsEqualTo(CacheTransportStatusCode.Unavailable);
        }
    }

    private static ClientPool CreatePool(Uri uri, FakeTimeProvider clock)
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "peer-0",
                Uri = uri,
            },
        };

        return new ClientPool(peers, static _ => new CallPolicy(TimeProvider.System), clock, connectOptions: FakeOnlyConnectOptions);
    }

    private static async Task AdvanceUntilCompletedAsync(FakeTimeProvider clock, Task operation)
    {
        // The real-time guard bounds a hang while the fake clock drives the retry budget.
        var guard = TimeProvider.System.GetTimestamp();
        while (!operation.IsCompleted && TimeProvider.System.GetElapsedTime(guard) < Bound)
        {
            clock.Advance(FakeOnlyConnectOptions.OverallDeadline);
            await Task.Yield();
        }
    }
}
