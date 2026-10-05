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

        var exception = await AsyncAssert.ThrowsAsync<InvalidOperationException, string>(new ValueTask<string>(warmUp.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));
        _ = await Assert.That(exception.Message).Contains("Failed to connect to endpoint", StringComparison.Ordinal);
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

        return new ClientPool(peers, static _ => new CallPolicy(), connectOptions: FakeOnlyConnectOptions, timeProvider: clock);
    }

    private static async Task AdvanceUntilCompletedAsync(FakeTimeProvider clock, Task operation)
    {
        // The real-time pause only paces the polling and the guard only bounds a hang; the outcome is decided by the fake clock.
        var guard = TimeProvider.System.GetTimestamp();
        while (!operation.IsCompleted && TimeProvider.System.GetElapsedTime(guard) < Bound)
        {
            clock.Advance(FakeOnlyConnectOptions.OverallDeadline);
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, CancellationToken.None);
        }
    }
}
