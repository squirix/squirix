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

    private static async Task<ReplicaGroupCommitPipeline> CreatePipelineAsync(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(GroupId, new byte[] { 9, 8, 7 }, 1, 1, GroupId, GroupId);
        var senders = ReplicaFollowerSenders.Create(
            gateway,
            [GroupId, "n2", "n3"],
            in status,
            in header,
            new ReplicaFollowerSenders.SenderTiming(HangGuard, HangGuard, TimeProvider.System),
            static _ => { });
        var lagging = new ReplicaLaggingFollowers(GroupId, new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance);
        return new ReplicaGroupCommitPipeline(new ReplicaLeaderApplier(new StubCache(), NullLogger.Instance), log, senders, GroupId, lagging, in status, 1);
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

    private async Task<FollowerLogs> OpenFollowersAsync(CancellationToken cancellationToken)
    {
        var followers = new FollowerLogs();
        try
        {
            foreach (var node in new[] { "n2", "n3" })
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
            foreach (var log in Values)
                await log.DisposeAsync();
        }
    }
}
