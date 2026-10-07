using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=3 group owner catches a follower that missed committed entries up from its log and admits it to the write quorum again.</summary>
public sealed class ReplicaFollowerCatchUpTests : IsolatedStorageTestBase
{
    private const int FollowerCaughtUpEventId = 4021;
    private const string GroupId = "n1";
    private const int VerificationCompleteEventId = 4002;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private string OwnerDir => Path.Join(Dir, "owner");

    /// <summary>
    /// After a restart the follower that missed every entry answers its probe as behind; one catch-up pass sends it the committed
    /// entries, admits it, and the group verifies as ready. A write after the admission reaches it through the resumed sender.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BehindFollowerIsCaughtUpAndAdmitted(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await SeedBehindFollowerAsync(gateway, cancellationToken);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Pending);
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);

        var admitted = await committer.CatchUpFollowersAsync(Reporter(), cancellationToken);

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        var status = await followers.Logs["n3"].GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.AllReady);

        var refusals = gateway.Refusals.Count;
        await committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await gateway.AppendedAsync("n3", 4).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(gateway.Refusals.Count).IsEqualTo(refusals);
    }

    /// <summary>A verification feeds one catch-up pass: a second pass without a new verification runs no session.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpRunsOncePerVerification(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await SeedBehindFollowerAsync(gateway, cancellationToken);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        _ = await Assert.That(await committer.CatchUpFollowersAsync(Reporter(), cancellationToken)).IsFalse();
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        _ = await Assert.That(await committer.CatchUpFollowersAsync(Reporter(), cancellationToken)).IsTrue();
        var arrivals = gateway.Arrivals.Count;

        _ = await Assert.That(await committer.CatchUpFollowersAsync(Reporter(), cancellationToken)).IsFalse();
        _ = await Assert.That(gateway.Arrivals.Count).IsEqualTo(arrivals);
    }

    /// <summary>A catch-up verified against a pipeline that a resync replaced since is not admitted into the new one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleAdmissionIsRefused(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await SeedBehindFollowerAsync(gateway, cancellationToken);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        var targets = committer.Probe.TakeCatchUpTargets();
        _ = await Assert.That(targets.Count).IsEqualTo(1);

        committer.DropStartedState();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var caughtUp = new ReplicaCatchUpResult(ReplicaCatchUpOutcome.CaughtUp, 3, 1, 0, 3, 2);

        var admitted = await committer.AdmitCaughtUpFollowerAsync(targets[0].ReplicaIndex, caughtUp, targets[0].Pipeline, cancellationToken);

        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
    }

    /// <summary>The readiness service catches the behind follower up on its own and reports the group verified.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadinessServiceCatchesUpFollower(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await SeedBehindFollowerAsync(gateway, cancellationToken);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        var log = new SignalingLogger(VerificationCompleteEventId);
        using var service = new ReplicaGroupReadinessService(committer, log, TimeProvider.System);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.Signaled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(FollowerCaughtUpEventId)).IsEqualTo(1);
        _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    private static ReplicaCatchUpReporter Reporter() => new(GroupId, NullLogger.Instance, null);

    /// <summary>
    /// Commits three writes while the append to n3 is parked, then disposes the owner, which cancels it: n2 holds the committed log
    /// and n3 holds nothing. The owner group log stays on disk for the restart under test.
    /// </summary>
    /// <param name="gateway">The follower transport double.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private async Task SeedBehindFollowerAsync(FollowerLogRoutingGateway gateway, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        gateway.ParkNext("n3");
        for (var i = 1; i <= 3; i++)
        {
            var key = $"k{i}";
            await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }

        await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    private async Task<Followers> OpenFollowersAsync(CancellationToken cancellationToken)
    {
        var followers = new Followers();
        try
        {
            foreach (var node in new[] { "n2", "n3" })
            {
                var log = new FollowerLog(Path.Join(Dir, node), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
                followers.Logs[node] = log;
                await log.OpenAsync(cancellationToken);
            }
        }
        catch
        {
            await followers.DisposeAsync();
            throw;
        }

        return followers;
    }

    /// <summary>The follower logs of the group, disposed together.</summary>
    private sealed class Followers : IAsyncDisposable
    {
        internal ConcurrentDictionary<string, FollowerLog> Logs { get; } = new(StringComparer.Ordinal);

        public async ValueTask DisposeAsync()
        {
            foreach (var log in Logs.Values)
                await log.DisposeAsync();
        }
    }

    /// <summary>Logger double that counts entries by event id and signals once an awaited event is logged.</summary>
    private sealed class SignalingLogger : ILogger<ReplicaGroupReadinessService>
    {
        private readonly ConcurrentQueue<int> _events = new();
        private readonly int _awaited;
        private readonly TaskCompletionSource _signaled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal SignalingLogger(int awaited)
        {
            _awaited = awaited;
        }

        /// <summary>Gets a task that completes once the awaited event was logged.</summary>
        internal Task Signaled => _signaled.Task;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _events.Enqueue(eventId.Id);
            if (eventId.Id == _awaited)
                _ = _signaled.TrySetResult();
        }

        internal int Count(int eventId)
        {
            var count = 0;
            foreach (var logged in _events)
            {
                if (logged == eventId)
                    count++;
            }

            return count;
        }
    }
}
