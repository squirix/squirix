using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Unit tests for the follower paths of <see cref="SquirixReplicationServiceAdapter" />.</summary>
[Immutable]
public sealed class SquirixReplicationFollowerAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that AdvanceReplicaCommit succeeds on a served group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceCommitOnServedGroupSucceedsAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var request = new AdvanceReplicaCommitRequest { Header = follower.Header, CommitIndex = 0 };

        var response = await follower.Adapter.AdvanceReplicaCommit(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Success).IsTrue();
        _ = await Assert.That(response.Term).IsEqualTo(7UL);
        _ = await Assert.That(response.CommitIndex).IsEqualTo(0UL);
        _ = await Assert.That(response.RefusalCode).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies that an empty append batch succeeds as a heartbeat.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyAppendOnServedGroupSucceedsAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var request = new AppendReplicaEntriesRequest { Header = follower.Header, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommitIndex = 0 };

        var response = await follower.Adapter.AppendReplicaEntries(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Success).IsTrue();
        _ = await Assert.That(response.Term).IsEqualTo(7UL);
        _ = await Assert.That(response.LastLogIndex).IsEqualTo(0UL);
        _ = await Assert.That(response.RefusalCode).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies that GetReplicaStatus reports a ready follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetStatusOnServedGroupReturnsReadyAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var request = new GetReplicaStatusRequest { Header = follower.Header };

        var response = await follower.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Readiness).IsEqualTo("ready");
        _ = await Assert.That(response.LastLogIndex).IsEqualTo(0UL);
        _ = await Assert.That(response.CommitIndex).IsEqualTo(0UL);
        _ = await Assert.That(response.RefusalCode).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies that InstallReplicaSnapshot refuses an unserved group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallOnUnknownGroupIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        follower.Header.GroupId = "unknown-group";
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 0 });

        var response = await follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Success).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Verifies that InstallReplicaSnapshot rejects a length-mismatched stream.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRejectsLengthMismatchAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 99 });

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that InstallReplicaSnapshot rejects a mismatched leader chunk.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallRejectsMismatchedLeaderChunkAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var other = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            SenderNodeId = "node-a",
            LeaderNodeId = "node-b",
            GroupId = "node-a",
            Term = 7,
        };
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(
            [new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 0 }, new InstallReplicaSnapshotRequest { Header = other }]);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext)));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that votes for the group this node statically leads are refused with failover on and leave its metadata unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupVoteIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync(CreateTopology().NodeId, true, cancellationToken);
        follower.Header.Term = 2;
        var request = new ReplicaVoteRequest { Header = follower.Header };
        var before = await File.ReadAllBytesAsync(MetaPath(follower), cancellationToken);

        var vote = await follower.Adapter.RequestVote(request, new TestServerCallContext(null, follower.HttpContext));
        var preVote = await follower.Adapter.PreVote(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(vote.Term).IsEqualTo(0UL);
        _ = await Assert.That(preVote.Granted).IsFalse();
        _ = await Assert.That(preVote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(preVote.Term).IsEqualTo(0UL);
        var after = await File.ReadAllBytesAsync(MetaPath(follower), cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>Verifies that a pre-vote on a served group answers from the log and leaves the term unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteOnServedGroupKeepsTermAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", true, cancellationToken);
        var request = new ReplicaVoteRequest { Header = follower.Header };

        var response = await follower.Adapter.PreVote(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Granted).IsTrue();
        _ = await Assert.That(response.Term).IsEqualTo(0UL);
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies that a vote for the static provisional leader term is refused without changing the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProvisionalTermVoteIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", true, cancellationToken);
        follower.Header.Term = 1;
        var request = new ReplicaVoteRequest { Header = follower.Header };

        var response = await follower.Adapter.RequestVote(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Granted).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo(RefusalCodes.StaleTerm);
        _ = await Assert.That(response.Term).IsEqualTo(0UL);
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies that GetReplicaStatus reports the term of the last log entry and the applied index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusReportsLogTermAndAppliedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var log = GroupLog(follower);
        FollowerLogEntry[] entries = [new(1UL, 7UL, new byte[] { 1 }), new(2UL, 7UL, new byte[] { 2 })];
        _ = await log.AppendAsync(new FollowerLogAppendRequest("node-a", 7UL, 0UL, 0UL, 2UL, entries), cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);
        var request = new GetReplicaStatusRequest { Header = follower.Header };

        var response = await follower.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(response.LastLogTerm).IsEqualTo(7UL);
        _ = await Assert.That(response.CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(response.AppliedIndex).IsEqualTo(1UL);
    }

    /// <summary>Verifies that a vote on a served group is granted to the verified sender and persisted before the answer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteOnServedGroupIsGrantedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", true, cancellationToken);
        follower.Header.Term = 2;
        var request = new ReplicaVoteRequest { Header = follower.Header };

        var response = await follower.Adapter.RequestVote(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(response.Granted).IsTrue();
        _ = await Assert.That(response.Term).IsEqualTo(2UL);
        _ = await Assert.That(response.RefusalCode).IsEqualTo(string.Empty);
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo("node-a");
    }

    /// <summary>Verifies that while automatic failover is off a vote on a served group is refused and changes no durable term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteWithFailoverOffChangesNoTermAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var request = new ReplicaVoteRequest { Header = follower.Header };

        var vote = await follower.Adapter.RequestVote(request, new TestServerCallContext(null, follower.HttpContext));
        var preVote = await follower.Adapter.PreVote(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(vote.Term).IsEqualTo(0UL);
        _ = await Assert.That(preVote.Granted).IsFalse();
        _ = await Assert.That(preVote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
    }

    /// <summary>Creates an adapter backed by an opened single-group registry, with automatic failover off.</summary>
    /// <param name="groupId">The replica group identifier served by the registry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The follower scope owning the adapter and its storage.</returns>
    private static Task<FollowerScope> CreateFollowerScopeAsync(string groupId, CancellationToken cancellationToken) =>
        CreateFollowerScopeAsync(groupId, false, cancellationToken);

    /// <summary>Creates an adapter backed by an opened single-group registry.</summary>
    /// <param name="groupId">The replica group identifier served by the registry.</param>
    /// <param name="votesEnabled">Whether automatic failover is on, so the adapter answers votes from the log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The follower scope owning the adapter and its storage.</returns>
    private static async Task<FollowerScope> CreateFollowerScopeAsync(string groupId, bool votesEnabled, CancellationToken cancellationToken)
    {
        var topology = CreateTopology(votesEnabled);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        var material = MtlsCertificate.Create(peerCertificate, bundle.Ca);
        var dir = new TempDirectory("squirix-replication-adapter");
        var registry = new ReplicaGroupRegistry(dir, [groupId], 1, ReadOnlyMemory<byte>.Of(9), topology.ConfigurationGeneration, NullLoggerFactory.Instance);
        await registry.OpenAsync(cancellationToken);
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material, registry);
        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            SenderNodeId = "node-a",
            LeaderNodeId = "node-a",
            GroupId = groupId,
            Term = 7,
            TopologyFingerprint = ByteString.CopyFromUtf8("\t"),
            ConfigurationGeneration = topology.ConfigurationGeneration,
        };
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = peerCertificate,
            },
        };
        return new FollowerScope
        {
            Adapter = adapter,
            Bundle = bundle,
            Dir = dir,
            Header = header,
            HttpContext = httpContext,
            Material = material,
            PeerCertificate = peerCertificate,
            Registry = registry,
        };
    }

    /// <summary>Gets the log of the group served by the scope.</summary>
    /// <param name="follower">The follower scope.</param>
    /// <returns>The group log.</returns>
    /// <exception cref="InvalidOperationException">The registry serves no log for the scope group.</exception>
    private static IFollowerLog GroupLog(FollowerScope follower) =>
        follower.Registry.TryGetLog(follower.Header.GroupId, out var log) ? log : throw new InvalidOperationException("The scope serves no group log.");

    /// <summary>Gets the metadata file path of the group served by the scope.</summary>
    /// <param name="follower">The follower scope.</param>
    /// <returns>The group metadata file path.</returns>
    private static string MetaPath(FollowerScope follower) => GroupStoragePaths.GetMetadataPath(follower.Dir, follower.Header.GroupId);

    /// <summary>Reads the durable status of the group served by the scope.</summary>
    /// <param name="follower">The follower scope.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The group log status.</returns>
    private static ValueTask<FollowerLogStatus> GroupStatusAsync(FollowerScope follower, CancellationToken cancellationToken) =>
        GroupLog(follower).GetStatusAsync(cancellationToken);

    [Immutable]
    private sealed class FollowerScope : IAsyncDisposable
    {
        internal required SquirixReplicationServiceAdapter Adapter { get; init; }

        internal required MtlsTestCertificateBundle Bundle { get; init; }

        internal required TempDirectory Dir { get; init; }

        internal required ReplicationEnvelopeHeader Header { get; init; }

        internal required DefaultHttpContext HttpContext { get; init; }

        internal required IDisposable Material { get; init; }

        internal required X509Certificate2 PeerCertificate { get; init; }

        internal required ReplicaGroupRegistry Registry { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Registry.DisposeAsync().ConfigureAwait(false);
            Dir.Dispose();
            Material.Dispose();
            PeerCertificate.Dispose();
            Bundle.Dispose();
        }
    }
}
