using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader-side re-drive of an uncommitted log tail to followers whose probe reported a log mismatch.</summary>
[Immutable]
public sealed class ReplicaTailRedriveTests : ServerUnitTestBase
{
    private const string GroupId = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    /// <summary>A follower holding the commit position but not the tail receives the tail and then holds exactly the leader log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RedriveAppendsMissingTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-tail-redrive-missing");
        await using var follower = await OpenFollowerAsync(dir, cancellationToken);
        _ = await follower.AppendAsync(Request(0, 0, Entry(1, 1)), cancellationToken);
        var tail = new ReplicaLeaderTail(1, 1, [Entry(2, 1), Entry(3, 1)]);

        var result = await ProbeAndRedriveAsync(follower, tail, 1, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaProbeResult(ReplicaProbeKind.Accepted, 3));
        await AssertHoldsAsync(follower, tail, cancellationToken);
    }

    /// <summary>A divergent uncommitted follower entry above its commit is replaced by the leader's tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RedriveReplacesDivergentTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-tail-redrive-divergent");
        await using var follower = await OpenFollowerAsync(dir, cancellationToken);
        _ = await follower.AppendAsync(Request(0, 0, Entry(1, 1)), cancellationToken);
        _ = await follower.AppendAsync(Request(1, 1, Entry(2, 1, "divergent")), cancellationToken);
        var tail = new ReplicaLeaderTail(1, 1, [Entry(2, 2), Entry(3, 2)]);

        var result = await ProbeAndRedriveAsync(follower, tail, 2, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaProbeResult(ReplicaProbeKind.Accepted, 3));
        await AssertHoldsAsync(follower, tail, cancellationToken);
    }

    /// <summary>A follower that does not hold the commit position is left mismatched and unchanged, for general catch-up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RedriveNeedsCommitPosition(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-tail-redrive-behind");
        await using var follower = await OpenFollowerAsync(dir, cancellationToken);
        var tail = new ReplicaLeaderTail(1, 1, [Entry(2, 1)]);

        var result = await ProbeAndRedriveAsync(follower, tail, 1, cancellationToken);

        _ = await Assert.That(result.Kind).IsEqualTo(ReplicaProbeKind.LogMismatch);
        _ = await Assert.That((await follower.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);
    }

    /// <summary>The leader's own slot is verified from its durable log even while that log carries an uncommitted tail.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task LeaderWithTailIsMarkedReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, in leader, Fingerprint, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

    private static async Task AssertHoldsAsync(FollowerLog follower, ReplicaLeaderTail tail, CancellationToken cancellationToken)
    {
        var held = await follower.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(held.Count).IsEqualTo(tail.Entries.Count);
        for (var i = 0; i < held.Count; i++)
        {
            _ = await Assert.That(held[i].Term).IsEqualTo(tail.Entries[i].Term);
            await SequenceAssert.EqualMemoryAsync(tail.Entries[i].Payload, held[i].Payload);
        }
    }

    private static FollowerLogEntry Entry(ulong logIndex, ulong term, string value = "v")
    {
        var record = new ReplicaLogRecord(
            logIndex,
            term,
            $"op-{logIndex}",
            "cache",
            new byte[] { 1 },
            "UserMutation",
            "cache",
            Encoding.UTF8.GetBytes("k"),
            "Set",
            Encoding.UTF8.GetBytes(value),
            ReadOnlyMemory<byte>.Empty,
            0,
            0,
            0,
            0);
        return new FollowerLogEntry(logIndex, term, ReplicaLogCodec.Encode(in record));
    }

    private static async Task<FollowerLog> OpenFollowerAsync(string dir, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        try
        {
            await log.OpenAsync(cancellationToken);
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }

        return log;
    }

    /// <summary>Probes the follower at the leader's last entry, as verification does, then re-drives the tail after the mismatch.</summary>
    /// <param name="follower">The follower log behind the transport double.</param>
    /// <param name="tail">The leader's uncommitted tail.</param>
    /// <param name="term">The leader's current term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The follower's verdict after the re-drive.</returns>
    private static async Task<ReplicaProbeResult> ProbeAndRedriveAsync(FollowerLog follower, ReplicaLeaderTail tail, ulong term, CancellationToken cancellationToken)
    {
        var gateway = new FollowerGateway(follower);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1, term, "n1", "n1");
        string[] members = ["n1", "n2"];
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, term, string.Empty, tail.LastIndex, tail.Entries[^1].Term, tail.CommitIndex, 0, FollowerLogReadiness.Ready);
        var probed = await ReplicaReadinessProbe.ProbeAllAsync(gateway, [false, true], members, header, leader, Timeout, cancellationToken);
        _ = await Assert.That(probed[1].Kind).IsEqualTo(ReplicaProbeKind.LogMismatch);

        var redriven = await ReplicaReadinessProbe.RedriveTailAsync(gateway, probed, members, header, tail, Timeout, cancellationToken);
        return redriven[1];
    }

    private static FollowerLogAppendRequest Request(ulong prevIndex, ulong prevTerm, FollowerLogEntry entry) =>
        new("n1", entry.Term, prevIndex, prevTerm, 0, new ReadOnlyMemory<FollowerLogEntry>([entry]));

    /// <summary>Follower transport double appending leader batches to a real follower log, as the follower RPC handler does.</summary>
    [Immutable]
    private sealed class FollowerGateway : IReplicaRpcGateway
    {
        private readonly FollowerLog _log;

        internal FollowerGateway(FollowerLog log)
        {
            _log = log;
        }

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var entries = new FollowerLogEntry[batch.Records.Count];
            for (var i = 0; i < entries.Length; i++)
            {
                var record = batch.Records[i];
                entries[i] = new FollowerLogEntry(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));
            }

            var request = new FollowerLogAppendRequest(batch.LeaderNodeId, batch.LeaderTerm, batch.PrevLogIndex, batch.PrevLogTerm, batch.LeaderCommitIndex, entries);
            return _log.AppendAsync(request, cancellationToken);
        }
    }
}
