using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
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
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>The offer of the owner of a group to take part in its first start, as the replication adapter answers it.</summary>
[Immutable]
public sealed class SquirixReplicationBootstrapAdapterTests : ServerUnitTestBase
{
    /// <summary>The offer of the owner is accepted by an unseeded copy and persisted, and the copy stays unseeded and ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerOfferIsAcceptedAndPersisted(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync("node-a", false, cancellationToken);
        var before = await GroupMetadataFile.ReadBytesAsync(MetaPath(scope), cancellationToken);

        var response = await scope.Adapter.OfferReplicaBootstrap(new OfferReplicaBootstrapRequest { Header = scope.Header }, scope.Context);

        _ = await Assert.That((response.Accepted, response.MemberState, response.RefusalCode)).IsEqualTo((true, "unseeded", string.Empty));
        var after = await GroupMetadataFile.ReadBytesAsync(MetaPath(scope), cancellationToken);
        _ = await Assert.That(Convert.ToBase64String(after, Base64FormattingOptions.None)).IsNotEqualTo(Convert.ToBase64String(before, Base64FormattingOptions.None));
        var status = await scope.Adapter.GetReplicaStatus(new GetReplicaStatusRequest { Header = scope.Header }, scope.Context);
        _ = await Assert.That((status.MemberState, status.Readiness)).IsEqualTo(("unseeded", "ready"));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(MetaPath(scope), cancellationToken)).BootstrapOffered).IsTrue();
    }

    /// <summary>A peer's offer and a peer's status call each wake the first start of the group this node owns, and record the sender as the peer that called.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PeerCallsRaiseTheWakeSignal(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync("node-a", false, cancellationToken);
        var quiet = await scope.Registry.PeerContact.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

        _ = await scope.Adapter.OfferReplicaBootstrap(new OfferReplicaBootstrapRequest { Header = scope.Header }, scope.Context);
        var afterOffer = await scope.Registry.PeerContact.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);
        var between = await scope.Registry.PeerContact.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await scope.Adapter.GetReplicaStatus(new GetReplicaStatusRequest { Header = scope.Header }, scope.Context);
        var afterStatus = await scope.Registry.PeerContact.WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

        _ = await Assert.That((quiet, afterOffer, between, afterStatus)).IsEqualTo((false, true, false, true));
        var contact = scope.Registry.PeerContact;
        _ = await Assert.That((contact.Contacted(scope.Header.SenderNodeId), contact.Contacted(scope.Header.LeaderNodeId))).IsEqualTo((true, false));
    }

    /// <summary>A member that is not the owner of the group cannot offer: the call is refused as not a member and nothing is recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferFromMemberThatIsNotOwnerIsRefused(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync("node-b", false, cancellationToken);
        var before = await GroupMetadataFile.ReadBytesAsync(MetaPath(scope), cancellationToken);

        var response = await scope.Adapter.OfferReplicaBootstrap(new OfferReplicaBootstrapRequest { Header = scope.Header }, scope.Context);

        _ = await Assert.That((response.Accepted, response.RefusalCode)).IsEqualTo((false, RefusalCodes.NotMember));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(MetaPath(scope), cancellationToken)).BootstrapOffered).IsFalse();
        var after = await GroupMetadataFile.ReadBytesAsync(MetaPath(scope), cancellationToken);
        await SequenceAssert.EqualAsync(before, after);
    }

    /// <summary>An offer under another topology is refused, names the state of the copy, and is not recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferWithWrongTopologyIsRefused(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync("node-a", false, cancellationToken);
        scope.Header.TopologyFingerprint = ByteString.CopyFromUtf8("x");

        var response = await scope.Adapter.OfferReplicaBootstrap(new OfferReplicaBootstrapRequest { Header = scope.Header }, scope.Context);

        _ = await Assert.That((response.Accepted, response.MemberState, response.RefusalCode)).IsEqualTo((false, "unseeded", RefusalCodes.TopologyMismatch));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(MetaPath(scope), cancellationToken)).BootstrapOffered).IsFalse();
    }

    /// <summary>A complete copy refuses the offer, and the reply names its state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferToCompleteCopyIsRefused(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync("node-a", true, cancellationToken);

        var response = await scope.Adapter.OfferReplicaBootstrap(new OfferReplicaBootstrapRequest { Header = scope.Header }, scope.Context);

        _ = await Assert.That((response.Accepted, response.MemberState, response.RefusalCode)).IsEqualTo((false, "complete", RefusalCodes.NotReady));
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(MetaPath(scope), cancellationToken)).BootstrapOffered).IsFalse();
    }

    private static string MetaPath(Scope scope) => GroupStoragePaths.GetMetadataPath(scope.Dir, scope.Header.GroupId);

    /// <summary>Creates an adapter over a registry that serves one group, whose copy is unseeded unless asked to be seeded.</summary>
    /// <param name="groupId">The served group; the caller is always the verified node-a.</param>
    /// <param name="seeded">Whether the copy is complete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scope owning the adapter and its storage.</returns>
    private static async Task<Scope> CreateScopeAsync(string groupId, bool seeded, CancellationToken cancellationToken)
    {
        var topology = CreateTopology();
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        var material = MtlsCertificate.Create(peerCertificate, bundle.Ca);
        var dir = new TempDirectory("squirix-replication-bootstrap-adapter");
        if (seeded)
            await SeededLogs.SeedGroupsAsync(dir, [groupId], cancellationToken);

        var registry = new ReplicaGroupRegistry(dir, [groupId], 1, ReadOnlyMemory<byte>.Of(9), topology.ConfigurationGeneration, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        await registry.OpenAsync(cancellationToken);
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material, TimeProvider.System, registry, RocksDoubles.CreateReplicaMembers());
        var httpContext = new DefaultHttpContext
        {
            Connection =
            {
                LocalPort = mtls.InternalListenPort,
                ClientCertificate = peerCertificate,
            },
        };
        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            SenderNodeId = "node-a",
            GroupId = groupId,
            TopologyFingerprint = ByteString.CopyFromUtf8("\t"),
            ConfigurationGeneration = topology.ConfigurationGeneration,
        };
        return new Scope
        {
            Adapter = adapter,
            Bundle = bundle,
            Context = new TestServerCallContext(null, httpContext),
            Dir = dir,
            Header = header,
            Material = material,
            PeerCertificate = peerCertificate,
            Registry = registry,
        };
    }

    [Immutable]
    private sealed class Scope : IAsyncDisposable
    {
        internal required SquirixReplicationServiceAdapter Adapter { get; init; }

        internal required MtlsTestCertificateBundle Bundle { get; init; }

        internal required TestServerCallContext Context { get; init; }

        internal required TempDirectory Dir { get; init; }

        internal required ReplicationEnvelopeHeader Header { get; init; }

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
