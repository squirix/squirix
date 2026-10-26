using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The deferred loop start of a <see cref="ReplicaFollowerSender" />: a run of entries is queued first and sent once.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderDeferredStartTests : ServerUnitTestBase
{
    private const int RunLength = 3;

    /// <summary>When the backlog fills in the middle of a run, the entries queued before it are still sent and acknowledged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BacklogFullMidRunSendsEarlierEntries(CancellationToken cancellationToken)
    {
        const int capacity = 2;
        var gateway = new ParkingFollowerGateway();
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { TimeProvider = TimeProvider.System, MaxPendingEntries = capacity };
        try
        {
            var run = EnqueueRun(sender);

            sender.StartPending();
            var request = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            request.Accept();
            _ = await BoundedAsync(run[0], cancellationToken);
            _ = await BoundedAsync(run[1], cancellationToken);

            _ = await Assert.That(request.Count).IsEqualTo(capacity);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(run[^1]);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Entries queued with the loop start deferred stay unsent until the sender is told to start.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeferredRunStaysUnsentUntilStarted(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var run = EnqueueRun(sender);

            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
            sender.StartPending();
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Accept();
            _ = await BoundedAsync(Task.WhenAll(run), cancellationToken);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An idle sender sends a deferred run as one request that names the entry before it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleSenderSendsRunAsOneRequest(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var run = EnqueueRun(sender);

            sender.StartPending();
            var request = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            request.Accept();
            var results = await BoundedAsync(Task.WhenAll(run), cancellationToken);

            _ = await Assert.That(request.Count).IsEqualTo(RunLength);
            _ = await Assert.That(request.FirstIndex).IsEqualTo(1UL);
            _ = await Assert.That(request.PrevLogIndex).IsEqualTo(0UL);
            _ = await Assert.That(results[^1].LogIndex).IsEqualTo(ulong.CreateChecked(RunLength));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A sender leased to a catch-up sends nothing on start; the run goes out once the lease ends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseEndingDuringDeferredRunStartsLoop(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var run = EnqueueRun(sender);

            sender.StartPending();
            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
            lease.Dispose();
            var request = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            request.Accept();
            _ = await BoundedAsync(Task.WhenAll(run), cancellationToken);

            _ = await Assert.That(request.Count).IsEqualTo(RunLength);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Starting a closed sender sends nothing, and the entries it still held are failed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task StartPendingOnClosedSenderSendsNothing(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        var run = EnqueueRun(sender);
        await sender.DisposeAsync();

        sender.StartPending();

        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(BoundedAsync(run[0], cancellationToken));
    }

    /// <summary>Entries admitted before a drain began are still sent by the start of their run, so the drain leaves none unsent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartPendingSendsWhileDraining(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var run = EnqueueRun(sender);
            await sender.DrainAsync(HangGuard);

            sender.StartPending();
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Accept();
            _ = await BoundedAsync(Task.WhenAll(run), cancellationToken);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Starting a sender with nothing queued sends nothing.</summary>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task StartPendingWithoutEntriesSendsNothing()
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            sender.StartPending();

            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    private static Task<ReplicaDurableAcknowledgement>[] EnqueueRun(ReplicaFollowerSender sender)
    {
        var run = new Task<ReplicaDurableAcknowledgement>[RunLength];
        for (var i = 0; i < run.Length; i++)
            run[i] = EnqueueAsync(sender, ulong.CreateChecked(i + 1), startLoop: false);

        return run;
    }
}
