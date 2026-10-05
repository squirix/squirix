using System;
using System.Net;
using System.Net.Sockets;
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

    /// <summary>Verifies caller cancellation in the middle of a connect attempt is cancellation, not a retried attempt failure.</summary>
    [Test]
    public async Task CallerCancelDuringAttemptIsCancellation()
    {
        using var listener = StartSilentListener();
        using var callerSource = new CancellationTokenSource();
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var pool = CreatePool(PeerUri(listener), clock);

        var warmUp = pool.WarmUpAsync(callerSource.Token);
        using var accepted = await listener.AcceptTcpClientAsync(callerSource.Token);
        await callerSource.CancelAsync();

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, string>(new ValueTask<string>(warmUp.AsTask().WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));
    }

    /// <summary>Verifies an attempt that outlives its timeout on the pool clock fails as an attempt timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AttemptTimeoutOnPoolClockFailsAttempt(CancellationToken cancellationToken)
    {
        using var listener = StartSilentListener();
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var pool = CreatePool(PeerUri(listener), clock);

        var warmUp = pool.WarmUpAsync(cancellationToken).AsTask();
        using var accepted = await listener.AcceptTcpClientAsync(cancellationToken);
        var pending = !warmUp.IsCompleted;
        await AdvanceUntilCompletedAsync(clock, warmUp);

        var exception = await AsyncAssert.ThrowsAsync<InvalidOperationException, string>(new ValueTask<string>(warmUp.WaitAsync(Bound, TimeProvider.System, CancellationToken.None)));
        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(exception.Message).Contains("per-attempt timeout", StringComparison.Ordinal);
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

    private static Uri PeerUri(TcpListener listener) =>
        new($"https://127.0.0.1:{(listener.LocalEndpoint is IPEndPoint endpoint ? endpoint.Port : 0)}");

    private static TcpListener StartSilentListener()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener;
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
