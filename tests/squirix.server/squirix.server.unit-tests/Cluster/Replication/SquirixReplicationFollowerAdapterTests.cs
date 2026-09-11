using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

using static Squirix.Server.UnitTests.Cluster.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Unit tests for the follower paths of <see cref="SquirixReplicationServiceAdapter" />.</summary>
[Immutable]
public sealed class SquirixReplicationFollowerAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that AdvanceReplicaCommit succeeds on a served group.</summary>
    [Fact]
    public async Task AdvanceCommitOnServedGroupSucceedsAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        var request = new AdvanceReplicaCommitRequest { Header = follower.Header, CommitIndex = 0 };

        var response = await follower.Adapter.AdvanceReplicaCommit(request, new TestServerCallContext(null, follower.HttpContext));

        Assert.True(response.Success);
        Assert.Equal(7UL, response.Term);
        Assert.Equal(0UL, response.CommitIndex);
        Assert.Equal(string.Empty, response.RefusalCode);
    }

    /// <summary>Verifies that an empty append batch succeeds as a heartbeat.</summary>
    [Fact]
    public async Task EmptyAppendOnServedGroupSucceedsAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        var request = new AppendReplicaEntriesRequest { Header = follower.Header, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommitIndex = 0 };

        var response = await follower.Adapter.AppendReplicaEntries(request, new TestServerCallContext(null, follower.HttpContext));

        Assert.True(response.Success);
        Assert.Equal(7UL, response.Term);
        Assert.Equal(0UL, response.LastLogIndex);
        Assert.Equal(string.Empty, response.RefusalCode);
    }

    /// <summary>Verifies that GetReplicaStatus reports a ready follower.</summary>
    [Fact]
    public async Task GetStatusOnServedGroupReturnsReadyAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        var request = new GetReplicaStatusRequest { Header = follower.Header };

        var response = await follower.Adapter.GetReplicaStatus(request, new TestServerCallContext(null, follower.HttpContext));

        Assert.Equal("ready", response.Readiness);
        Assert.Equal(0UL, response.LastLogIndex);
        Assert.Equal(0UL, response.CommitIndex);
        Assert.Equal(string.Empty, response.RefusalCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot refuses an unserved group.</summary>
    [Fact]
    public async Task InstallOnUnknownGroupIsRefusedAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        follower.Header.GroupId = "unknown-group";
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>([new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 0 }]);

        var response = await follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext));

        Assert.False(response.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, response.RefusalCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot rejects a mismatched leader chunk.</summary>
    [Fact]
    public async Task InstallRejectsMismatchedLeaderChunkAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        var other = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            SenderNodeId = "node-a",
            LeaderNodeId = "node-b",
            GroupId = "node-a",
            Term = 7,
        };
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>(
            [new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 0 }, new InstallReplicaSnapshotRequest { Header = other }]);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext)));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Verifies that InstallReplicaSnapshot rejects a length-mismatched stream.</summary>
    [Fact]
    public async Task InstallRejectsLengthMismatchAsync()
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", DefaultCancellationToken);
        var stream = new TestAsyncStreamReader<InstallReplicaSnapshotRequest>([new InstallReplicaSnapshotRequest { Header = follower.Header, TotalBytes = 99 }]);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.InstallReplicaSnapshot(stream, new TestServerCallContext(null, follower.HttpContext)));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>Creates an adapter backed by an opened single-group registry.</summary>
    /// <param name="groupId">The replica group identifier served by the registry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The follower scope owning the adapter and its storage.</returns>
    private static async Task<FollowerScope> CreateFollowerScopeAsync(string groupId, CancellationToken cancellationToken)
    {
        var topology = CreateTopology();
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        var material = MtlsCertificateMaterial.Create(peerCertificate, bundle.Ca);
        var dir = new TempDirectory("squirix-replication-adapter");
        var registry = new ReplicaGroupRegistry(dir, [groupId], 1, new ReadOnlyMemory<byte>([9]), topology.ConfigurationGeneration);
        await registry.OpenAsync(cancellationToken);
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material, registry);
        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
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

    /// <summary>In-memory <see cref="IAsyncStreamReader{T}" /> backed by a fixed item sequence.</summary>
    /// <typeparam name="T">The streamed message type.</typeparam>
    [Immutable]
    private sealed class TestAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IEnumerator<T> _items;

        /// <summary>Initializes a new instance of the <see cref="TestAsyncStreamReader{T}" /> class.</summary>
        /// <param name="items">The item sequence to stream.</param>
        internal TestAsyncStreamReader(IEnumerable<T> items)
        {
            _items = items.GetEnumerator();
        }

        /// <inheritdoc />
        public T Current => _items.Current;

        /// <inheritdoc />
        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(_items.MoveNext());
    }
}
