using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A slow recovering follower does not stop writes backed by the healthy majority.</summary>
public sealed class ReplicaCatchUpAvailabilityTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>A hung live append cannot keep the healthy majority behind the catch-up lease acquisition.</summary>
    /// <param name="token">The test cancellation token.</param>
    [Test]
    public async Task BusySenderDoesNotHoldTheWriteGate(CancellationToken token)
    {
        await using var n2 = new FollowerLog(Path.Join(Dir, "n2"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await using var n3 = new FollowerLog(Path.Join(Dir, "n3"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await n2.OpenSeededAsync(token);
        await n3.OpenSeededAsync(token);
        var routing = new FollowerLogRoutingGateway(new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal) { ["n2"] = n2, ["n3"] = n3 });
        var owner = Path.Join(Dir, "owner");
        await SeedAsync(owner, routing, token, 3);
        var gateway = new ReplayParkingGateway(routing) { FirstIndex = 4, IgnoreCancellation = true };
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(owner, token);
        await using var committer = CreateCommitterOnBudgetClock(registry, gateway, clock);
        _ = await committer.VerifyReplicasAsync(token);
        _ = await committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n1", NullLogger.Instance, null), token);
        try
        {
            await committer.CommitSetAsync(NewOperationId(), "cache", "live", Entry("live"), token).WaitAsync(Bound, TimeProvider.System, token);
            await gateway.Parked.WaitAsync(Bound, TimeProvider.System, token);
            registry.EligibilityFor("n1").Forget(2);
            committer.Probe.OfferCatchUp([false, false, true], committer.RunningPipeline!.CatchUpTargetFor);
            var repairing = committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n1", NullLogger.Instance, null), token);
            clock.Advance(ReplicaVerificationProbe.ProbeTimeout);
            _ = await Assert.That(await repairing.WaitAsync(Bound, TimeProvider.System, token)).IsFalse();
            await committer.CommitSetAsync(NewOperationId(), "cache", "after-busy", Entry("after-busy"), token).WaitAsync(Bound, TimeProvider.System, token);
            _ = await Assert.That((await n2.GetStatusAsync(token)).LastLogIndex).IsEqualTo(5UL);
        }
        finally
        {
            gateway.Release();
        }
    }

    /// <summary>A slow follower yields after bounded finalization attempts instead of monopolizing the repair loop.</summary>
    /// <param name="token">The test cancellation token.</param>
    [Test]
    public async Task SlowFinalizationYieldsToTheRepairLoop(CancellationToken token)
    {
        await using var n2 = new FollowerLog(Path.Join(Dir, "n2"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await using var n3 = new FollowerLog(Path.Join(Dir, "n3"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await n2.OpenSeededAsync(token);
        await n3.OpenSeededAsync(token);
        var routing = new FollowerLogRoutingGateway(new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal) { ["n2"] = n2, ["n3"] = n3 });
        var owner = Path.Join(Dir, "owner");
        await SeedAsync(owner, routing, token, 3);
        var gateway = new SlowFinalizationGateway(routing);
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(owner, token);
        await using var committer = CreateCommitterOnBudgetClock(registry, gateway, clock);
        _ = await committer.VerifyReplicasAsync(token);
        gateway.Armed = true;
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(token);
        var repairing = committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n1", NullLogger.Instance, null), stopping.Token);
        try
        {
            for (var attempt = 0; attempt < 2; attempt++)
            {
                _ = await gateway.Parked.Reader.ReadAsync(token).AsTask().WaitAsync(Bound, TimeProvider.System, token);
                clock.Advance(ReplicaVerificationProbe.ProbeTimeout);
            }

            _ = await Assert.That(await repairing.WaitAsync(Bound, TimeProvider.System, token)).IsFalse();
            _ = await Assert.That(registry.EligibilityFor("n1").StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
            await committer.CommitSetAsync(NewOperationId(), "cache", "after-yield", Entry("after-yield"), token).WaitAsync(Bound, TimeProvider.System, token);
            _ = await Assert.That((await n2.GetStatusAsync(token)).LastLogIndex).IsEqualTo(4UL);
        }
        finally
        {
            await stopping.CancelAsync();
            Task completion = repairing;
            await completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    /// <summary>A write commits while a later replay batch is held, before the recovering follower is admitted.</summary>
    /// <param name="token">The test cancellation token.</param>
    [Test]
    public async Task HealthyMajorityWritesDuringLongReplay(CancellationToken token)
    {
        await using var n2 = new FollowerLog(Path.Join(Dir, "n2"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await using var n3 = new FollowerLog(Path.Join(Dir, "n3"), "n1", GroupComposition.Create("n1"), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await n2.OpenSeededAsync(token);
        await n3.OpenSeededAsync(token);
        var routing = new FollowerLogRoutingGateway(new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal) { ["n2"] = n2, ["n3"] = n3 });
        var owner = Path.Join(Dir, "owner");
        await SeedAsync(owner, routing, token);
        var gateway = new ReplayParkingGateway(routing);
        await using var registry = await OpenRegistryAsync(owner, token);
        await using var committer = CreateCommitter(registry, gateway);
        _ = await committer.VerifyReplicasAsync(token);
        var repairing = committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n1", NullLogger.Instance, null), token);
        Task? writing = null;
        try
        {
            await gateway.Parked.WaitAsync(Bound, TimeProvider.System, token);
            _ = await Assert.That(registry.EligibilityFor("n1").StateFor(2)).IsEqualTo(ReplicaParticipantState.CatchingUp);
            writing = committer.CommitSetAsync(NewOperationId(), "cache", "during-replay", Entry("during-replay"), token);
            await writing.WaitAsync(committer.CommitBudget, TimeProvider.System, token);
            _ = await Assert.That(repairing.IsCompleted).IsFalse();
            _ = await Assert.That((await n2.GetStatusAsync(token)).LastLogIndex).IsEqualTo(131UL);
            gateway.Release();
            _ = await Assert.That(await repairing.WaitAsync(Bound, TimeProvider.System, token)).IsTrue();
            _ = await Assert.That((await n3.GetStatusAsync(token)).LastLogIndex).IsEqualTo(131UL);
        }
        finally
        {
            gateway.Release();
            if (writing != null)
                await writing.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _ = await repairing.WaitAsync(Bound, TimeProvider.System, token);
        }
    }

    private static async Task SeedAsync(string owner, FollowerLogRoutingGateway routing, CancellationToken token, int count = 130)
    {
        await using var registry = await OpenRegistryAsync(owner, token);
        await using var committer = CreateCommitter(registry, routing);
        routing.ParkNext("n3");
        for (var index = 1; index <= count; index++)
            await committer.CommitSetAsync(NewOperationId(), "cache", $"k{index}", Entry($"k{index}"), token);
        await routing.Parked.WaitAsync(Bound, TimeProvider.System, token);
    }

    private sealed class SlowFinalizationGateway : IReplicaRpcGateway
    {
        private readonly IReplicaRpcGateway _routing;
        private int _confirmations;

        internal SlowFinalizationGateway(IReplicaRpcGateway routing)
        {
            _routing = routing;
        }

        internal bool Armed { get; set; }

        internal Channel<int> Parked { get; } = Channel.CreateUnbounded<int>();

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var result = await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken);
            if (Armed && string.Equals(nodeId, "n3", StringComparison.Ordinal) && batch.Entries.Count == 0 && result.Success && Interlocked.Increment(ref _confirmations) % 2 == 1)
            {
                _ = Parked.Writer.TryWrite(_confirmations);
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken);
            }

            return result;
        }
    }

    private sealed class ReplayParkingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IReplicaRpcGateway _routing;
        private int _armed = 1;

        internal ReplayParkingGateway(IReplicaRpcGateway routing)
        {
            _routing = routing;
        }

        internal Task Parked => _parked.Task;

        internal ulong FirstIndex { get; init; } = 129;

        internal bool IgnoreCancellation { get; init; }

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var result = await _routing.AppendEntriesAsync(nodeId, header, batch, cancellationToken);
            if (string.Equals(nodeId, "n3", StringComparison.Ordinal) && batch.Entries.Count > 0 && batch.Entries[0].LogIndex >= FirstIndex && Interlocked.Exchange(ref _armed, 0) == 1)
            {
                _ = _parked.TrySetResult();
                if (IgnoreCancellation)
                    await _release.Task;
                else
                    await _release.Task.WaitAsync(cancellationToken);
            }

            return result;
        }

        internal void Release() => _ = _release.TrySetResult();
    }
}
