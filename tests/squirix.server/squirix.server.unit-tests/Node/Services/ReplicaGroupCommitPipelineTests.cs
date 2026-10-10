using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The owner commit pipeline queues every locally appended entry on the follower senders as part of the local append.</summary>
public sealed class ReplicaGroupCommitPipelineTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The local append alone sends the entry to every follower, so an exception between the append and the fan-out of the coordinator
    /// cannot leave it unsent; the fan-out then takes the queued acknowledgements.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LocalAppendQueuesEveryFollower(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            var mutation = Mutation(1);

            await pipeline.AppendLocalAsync(mutation, cancellationToken);

            await routing.AppendedAsync("n2", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var acknowledgement = await pipeline.AppendFollowerAsync(2, mutation, cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(acknowledgement.LogIndex).IsEqualTo(1UL);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>The fan-out of an entry this pipeline did not append last is refused instead of sending it out of order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FanOutOfOtherEntryIsRefused(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            await pipeline.AppendLocalAsync(Mutation(1), cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.AppendFollowerAsync(1, Mutation(2), cancellationToken).AsTask());
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>
    /// A leader at slot 1 sends to slots 0 and 2 through its two senders: the fan-out of slot 2 takes the acknowledgement of the sender of
    /// n3, and a lagging slot 0 is reported as n1.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderAtSlotOneMapsSenders(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken, "n1", "n3");
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var eligibility = new ReplicaEligibility(3);
        var ready = new ReplicaProgress(1, 0, 0, 0, 0, new byte[] { 9, 8, 7 }, 1, 0);
        _ = eligibility.TryMarkReady(0, in ready, in ready);
        var logger = new EventRecordingLogger();
        var lagging = new ReplicaLaggingFollowers(GroupId, eligibility, new ReplicaRepairQueue(3), logger);
        var pipeline = await CreatePipelineAsync(registry, routing, ("n2", 1), lagging, cancellationToken);
        try
        {
            var mutation = Mutation(1);
            routing.ParkNext("n3");

            await pipeline.AppendLocalAsync(mutation, cancellationToken);
            await routing.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var toFirst = FanOutAsync(pipeline, 0, mutation, cancellationToken);
            var toThird = FanOutAsync(pipeline, 2, mutation, cancellationToken);
            var first = await toFirst.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var parked = toThird.IsCompleted;
            routing.Release();
            var third = await toThird.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            pipeline.RecordLaggingReplica(0, 1);

            _ = await Assert.That((first.LogIndex, third.LogIndex, parked)).IsEqualTo((1UL, 1UL, false));
            _ = await Assert.That(logger.FindMessage(4026)).Contains("follower n1 ", StringComparison.Ordinal);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    private static Task<ReplicaDurableAcknowledgement> FanOutAsync(ReplicaGroupCommitPipeline pipeline, int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        pipeline.AppendFollowerAsync(replicaIndex, mutation, cancellationToken).AsTask();

    private static Task<ReplicaGroupCommitPipeline> CreatePipelineAsync(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, CancellationToken cancellationToken) =>
        CreatePipelineAsync(
            registry,
            gateway,
            (GroupId, 0),
            new ReplicaLaggingFollowers(GroupId, new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance),
            cancellationToken);

    private static async Task<ReplicaGroupCommitPipeline> CreatePipelineAsync(
        ReplicaGroupRegistry registry,
        IReplicaRpcGateway gateway,
        (string SelfId, int ReplicaIndex) leader,
        ReplicaLaggingFollowers lagging,
        CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(GroupId, new byte[] { 9, 8, 7 }, 1, 1, leader.SelfId, leader.SelfId);
        var senders = ReplicaFollowerSenders.Create(
            gateway,
            [GroupId, "n2", "n3"],
            leader.ReplicaIndex,
            in status,
            in header,
            new ReplicaFollowerSenders.SenderTiming(HangGuard, HangGuard, TimeProvider.System),
            static _ => { });
        return new ReplicaGroupCommitPipeline(new ReplicaGroupApplier(new StubCache(), NullLogger.Instance), log, senders, leader, lagging, in status, 1);
    }

    private static PreparedReplicaMutation Mutation(ulong logIndex)
    {
        var record = new ReplicaLogRecord(
            logIndex,
            1,
            $"op-{logIndex}",
            "client",
            new byte[] { 1 },
            "UserMutation",
            "cache",
            Encoding.UTF8.GetBytes("k"),
            "Set",
            Encoding.UTF8.GetBytes("v"),
            ReadOnlyMemory<byte>.Empty,
            0,
            0,
            0,
            0);
        var identity = new ReplicaOperationIdentity(GroupId, "client", $"op-{logIndex}", new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, logIndex, new ReplicaMutationPayload(ReplicaLogCodec.Encode(in record), new byte[] { 3 }, 4));
    }

    private Task<FollowerLogs> OpenFollowersAsync(CancellationToken cancellationToken) => OpenFollowersAsync(cancellationToken, "n2", "n3");

    private async Task<FollowerLogs> OpenFollowersAsync(CancellationToken cancellationToken, params string[] nodes)
    {
        var followers = new FollowerLogs();
        try
        {
            foreach (var node in nodes)
            {
                var log = new FollowerLog(Path.Join(Dir, node), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
                followers[node] = log;
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

    /// <summary>The follower logs of the group by node, disposed together.</summary>
    private sealed class FollowerLogs : ConcurrentDictionary<string, FollowerLog>, IAsyncDisposable
    {
        internal FollowerLogs()
            : base(StringComparer.Ordinal)
        {
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var log in this)
                await log.Value.DisposeAsync();
        }
    }
}
