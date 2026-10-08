using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
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

    /// <summary>
    /// A caught-up follower whose admission waits for the commit gate gives the admission up once its sender starts draining, so the
    /// drain of a resync under that gate is not held for its whole budget.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainDuringAdmissionIsNotBlocked(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        await SeedBehindFollowerAsync(routing, cancellationToken);
        var gateway = new ConfirmationParkingGateway(routing, "n3", 3);
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(OwnerDir, new FollowerLogOptions { FaultHooks = hooks }, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        try
        {
            _ = await committer.VerifyReplicasAsync(cancellationToken);
            var target = committer.Probe.TakeCatchUpTargets()[0];
            committer.Probe.OfferCatchUp([false, false, true], _ => target);
            var log = new EventRecordingLogger();
            var catchUp = committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter(GroupId, log, null), cancellationToken);
            await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // A write holds the commit gate, stalled at its local append, when the session's confirmation is answered.
            hooks.StallNextFrameWrite();
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken);
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            gateway.Release();

            await target.Sender.DrainAsync(HangGuard).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(await catchUp.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
            _ = await Assert.That(log.FindMessage(4025)).Contains("caught_up");
            _ = await Assert.That(write.IsCompleted).IsFalse();
            hooks.Release();
            await write.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
            hooks.Release();
        }
    }

    /// <summary>
    /// A ready follower whose append fails is taken out of the quorum and queued for repair; one verification and catch-up pass
    /// brings it back without a leader restart, and the next write reaches it through the resumed sender.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedAppendIsRepairedWithoutRestart(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        var gateway = new FailingGateway(routing);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await CommitAsync(committer, "k1", cancellationToken);
        await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        gateway.FailNext("n3");
        gateway.ReleaseFailure();
        await CommitAsync(committer, "k2", cancellationToken);

        _ = await Assert.That(await committer.Probe.Repairs.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        _ = await Assert.That(await committer.CatchUpFollowersAsync(Reporter(), cancellationToken)).IsTrue();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);

        await CommitAsync(committer, "k3", cancellationToken);
        await routing.AppendedAsync("n3", 3).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    /// <summary>A follower that is only slower than the majority stays in the quorum, and its late acknowledgement is recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowFollowerIsNotDemoted(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, routing);
        try
        {
            await CommitAsync(committer, "k1", cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            routing.ParkNext("n3");
            await CommitAsync(committer, "k2", cancellationToken);
            await routing.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
            routing.Release();
            await routing.AppendedAsync("n3", 2).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await committer.Probe.Repairs.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken)).IsFalse();
            _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        }
        finally
        {
            routing.Release();
        }
    }

    /// <summary>An append that times out on the sender's clock takes the follower out of the quorum and queues it for repair.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutAppendIsDemoted(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitterOnBudgetClock(registry, routing, clock);
        try
        {
            await CommitAsync(committer, "k1", cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            routing.ParkNext("n3");
            await CommitAsync(committer, "k2", cancellationToken);
            await routing.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            clock.Advance(committer.CommitBudget);

            await routing.ParkCanceled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(await committer.Probe.Repairs.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
            _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        }
        finally
        {
            routing.Release();
        }
    }

    /// <summary>
    /// A follower whose append hangs, ignoring its cancellation, is never seen to fail, so it is still ready, behind the commit index,
    /// when the coordinator is replaced. The new start verifies it again instead of taking it to hold the log through the commit index,
    /// and compaction keeps the entries it lacks.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HungFollowerIsVerifiedOnRestart(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        var gateway = new HangingGateway(routing);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitterOnBudgetClock(registry, gateway, new FakeTimeProvider(), TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        try
        {
            await CommitAsync(committer, "k1", cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            gateway.HangNext("n3");
            var operationId = NewOperationId();
            await committer.CommitSetAsync(operationId, "cache", "k2", Entry("k2"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await gateway.Hung.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // The retry replaces the coordinator and is answered from the retained outcome, so nothing new is appended to expose n3.
            committer.DropStartedState();
            await committer.CommitSetAsync(operationId, "cache", "k2", Entry("k2"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
            _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
            var outcome = await committer.CompactOwnedLogAsync(new ReplicaLogCompactionPolicy(long.MaxValue, 1), DurableJournal(), cancellationToken);
            _ = await Assert.That(outcome).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady);
        }
        finally
        {
            gateway.Release();
        }
    }

    /// <summary>The readiness service, woken by the demotion, repairs the follower whose append failed on its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadinessServiceRepairsDemoted(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers.Logs);
        var gateway = new FailingGateway(routing);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await CommitAsync(committer, "k1", cancellationToken);
        await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var log = new SignalingLogger();
        using var service = new ReplicaGroupReadinessService(LeadOwn(committer), log, TimeProvider.System);
        await service.StartAsync(cancellationToken);
        try
        {
            // The first pass finds the group ready; the failure below then reaches it only through the repair queue.
            // The failure is reported only once the write is committed: the woken service then finds no uncommitted tail to re-send,
            // and the follower can only come back through its catch-up session.
            await log.LoggedAsync(VerificationCompleteEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            gateway.FailNext("n3");
            await CommitAsync(committer, "k2", cancellationToken);
            gateway.ReleaseFailure();

            await log.LoggedAsync(FollowerCaughtUpEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
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
        var log = new SignalingLogger();
        using var service = new ReplicaGroupReadinessService(LeadOwn(committer), log, TimeProvider.System);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.LoggedAsync(VerificationCompleteEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(FollowerCaughtUpEventId)).IsEqualTo(1);
        _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    private static IJournalDurabilityCoordinator DurableJournal()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static ReplicaCatchUpReporter Reporter() => new(GroupId, NullLogger.Instance, null);

    private static Task CommitAsync(ReplicaGroupCommitter committer, string key, CancellationToken cancellationToken) =>
        committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

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

    /// <summary>Routes appends to the follower logs and fails the next batch with entries to a chosen follower in transport.</summary>
    private sealed class FailingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IReplicaRpcGateway _routing;
        private string? _failNode;

        internal FailingGateway(IReplicaRpcGateway routing)
        {
            _routing = routing;
        }

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Records.Count == 0 || !string.Equals(Interlocked.CompareExchange(ref _failNode, null, nodeId), nodeId, StringComparison.Ordinal))
                return await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            throw new IOException($"Injected transport failure to {nodeId}.");
        }

        /// <summary>Makes the next batch with entries sent to <paramref name="node" /> fail in transport once <see cref="ReleaseFailure" /> runs.</summary>
        /// <param name="node">The follower node.</param>
        internal void FailNext(string node) => Volatile.Write(ref _failNode, node);

        /// <summary>Lets the armed failure, and every later one, be reported.</summary>
        internal void ReleaseFailure() => _ = _release.TrySetResult();
    }

    /// <summary>Routes appends to the follower logs and holds the next batch with entries to a chosen follower, ignoring its cancellation.</summary>
    private sealed class HangingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _hung = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IReplicaRpcGateway _routing;
        private string? _hangNode;

        internal HangingGateway(IReplicaRpcGateway routing)
        {
            _routing = routing;
        }

        /// <summary>Gets a task that completes once the armed batch hangs.</summary>
        internal Task Hung => _hung.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Records.Count == 0 || !string.Equals(Interlocked.CompareExchange(ref _hangNode, null, nodeId), nodeId, StringComparison.Ordinal))
                return await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            // A follower call that hangs this way is given up on by the sender's teardown, so its acknowledgement never completes.
            _ = _hung.TrySetResult();
            await _release.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            throw new IOException($"Released hung call to {nodeId}.");
        }

        /// <summary>Makes the next batch with entries sent to <paramref name="node" /> hang until <see cref="Release" /> runs.</summary>
        /// <param name="node">The follower node.</param>
        internal void HangNext(string node) => Volatile.Write(ref _hangNode, node);

        /// <summary>Ends the hung call.</summary>
        internal void Release() => _ = _release.TrySetResult();
    }

    /// <summary>Routes appends to the follower logs and parks the first accepted empty append that confirms a follower holds an index.</summary>
    private sealed class ConfirmationParkingGateway : IReplicaRpcGateway
    {
        private readonly string _node;
        private readonly ulong _index;
        private readonly TaskCompletionSource _parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IReplicaRpcGateway _routing;
        private int _armed = 1;

        internal ConfirmationParkingGateway(IReplicaRpcGateway routing, string node, ulong index)
        {
            _routing = routing;
            _node = node;
            _index = index;
        }

        /// <summary>Gets a task that completes once the confirmation parked.</summary>
        internal Task Parked => _parked.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var result = await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
            if (batch.Records.Count == 0 && result.Success && result.LastLogIndex == _index && string.Equals(nodeId, _node, StringComparison.Ordinal) &&
                Interlocked.Exchange(ref _armed, 0) == 1)
            {
                _ = _parked.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        /// <summary>Lets the parked confirmation return to the session.</summary>
        internal void Release() => _ = _release.TrySetResult();
    }

    /// <summary>Logger double that counts entries by event id and signals each event id once it is logged.</summary>
    private sealed class SignalingLogger : ILogger<ReplicaGroupReadinessService>
    {
        private readonly ConcurrentQueue<int> _events = new();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _logged = new();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _events.Enqueue(eventId.Id);
            _ = Signal(eventId.Id).TrySetResult();
        }

        /// <summary>Gets a task that completes once an entry with <paramref name="eventId" /> is logged.</summary>
        /// <param name="eventId">The event id.</param>
        /// <returns>The task.</returns>
        internal Task LoggedAsync(int eventId) => Signal(eventId).Task;

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

        private TaskCompletionSource Signal(int eventId) =>
            _logged.GetOrAdd(eventId, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}
