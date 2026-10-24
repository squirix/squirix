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
using Squirix.Server.TestKit.Replication;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Owner pipelines over real follower logs, shared by the commit pipeline tests.</summary>
internal static class ReplicaPipelineTestKit
{
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    internal static Task<ReplicaGroupCommitPipeline> CreatePipelineAsync(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, CancellationToken cancellationToken) =>
        CreatePipelineAsync(
            registry,
            gateway,
            (GroupId, 0),
            new ReplicaLaggingFollowers(GroupId, new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance),
            cancellationToken);

    internal static async Task<ReplicaGroupCommitPipeline> CreatePipelineAsync(
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

    internal static PreparedReplicaMutation Mutation(ulong logIndex)
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

    internal static async Task<FollowerLogs> OpenFollowersAsync(string dir, CancellationToken cancellationToken, params string[] nodes)
    {
        var followers = new FollowerLogs();
        try
        {
            foreach (var node in nodes.Length > 0 ? nodes : ["n2", "n3"])
            {
                var log = new FollowerLog(Path.Join(dir, node), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
                followers[node] = log;
                await log.OpenSeededAsync(cancellationToken);
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
    internal sealed class FollowerLogs : ConcurrentDictionary<string, FollowerLog>, IAsyncDisposable
    {
        internal FollowerLogs()
            : base(StringComparer.Ordinal)
        {
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var (_, log) in this)
                await log.DisposeAsync();
        }
    }
}
