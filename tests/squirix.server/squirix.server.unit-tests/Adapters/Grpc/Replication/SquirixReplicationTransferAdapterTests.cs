using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using WireEnd = Squirix.Server.Adapters.Grpc.Replication.ReplicaTransferEnd;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>A state transfer as the replication adapter receives it: who may send it, how each step reaches the receiver, and how its answer maps back.</summary>
[Immutable]
public sealed class SquirixReplicationTransferAdapterTests : ServerUnitTestBase
{
    private const string GroupId = TransferFixtures.GroupId;
    private const string LeaderId = "n2";
    private const string Transfer = "transfer-1";
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(5);

    /// <summary>Every step of a transfer reaches the receiver in order with its values, the wipe reports more until the group is empty, and the copy ends complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryStepReachesReceiver(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 2 });
        scope.Sink.Keys = 3;

        var replies = await RunAsync(scope);

        _ = await Assert.That(replies.Length).IsEqualTo(7);
        _ = await Assert.That(Array.TrueForAll(replies, static r => r.Success && r.RefusalCode.Length == 0)).IsTrue();
        _ = await Assert.That((replies[3].More, replies[4].More)).IsEqualTo((true, false));
        _ = await Assert.That(replies[6].Term).IsEqualTo(1UL);
        await SequenceAssert.EqualAsync(["enter", "wipe", "wipe", "write", "seal", "reset", "leave"], scope.Sink.Calls, StringComparer.Ordinal);
        _ = await Assert.That(scope.Sink.ResetIndex).IsEqualTo(TransferFixtures.SnapshotIndex);
        var written = scope.Sink.Written;
        _ = await Assert.That(written.Count).IsEqualTo(2);
        _ = await Assert.That((written[0].CacheName, Encoding.UTF8.GetString(written[0].Key.Span), written[0].Payload.ToArray().Length, written[0].ExpiresUtcTicks)).IsEqualTo(("cache", "k-1", 2, 0L));
        _ = await Assert.That((written[1].CacheName, Encoding.UTF8.GetString(written[1].Key.Span), written[1].Payload.ToArray().Length, written[1].ExpiresUtcTicks)).IsEqualTo(("other", "k-2", 2, 123456L));
        _ = await Assert.That(scope.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
    }

    /// <summary>An accepted transfer message counts as a contact of the leader for the election state of the group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedMessageIsLeaderContact(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var election = scope.Registry.StateFor(GroupId);
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsFalse();

        var reply = await scope.Adapter.TransferReplicaState(BeginRequest(scope, TransferFixtures.Image()), scope.Context);

        _ = await Assert.That(reply.Success).IsTrue();
        _ = await Assert.That(election.HasRecentLeaderContact(Window)).IsTrue();
    }

    /// <summary>A transfer that completes wakes the apply loop of the group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedTransferRaisesApplySignal(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        _ = await RunAsync(scope);

        var raised = await scope.Registry.ApplySignalFor(GroupId).WaitAsync(TimeSpan.FromHours(1), scope.Clock, cancellationToken);

        _ = await Assert.That(raised).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>A group this node does not serve is refused as not a member, and the refusal is a reply with term zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnknownGroupIsRefusedAsNotMember(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var request = BeginRequest(scope, TransferFixtures.Image());
        request.Header.GroupId = "other-group";

        var reply = await scope.Adapter.TransferReplicaState(request, scope.Context);

        _ = await Assert.That((reply.Success, reply.RefusalCode, reply.Term)).IsEqualTo((false, RefusalCodes.NotMember, 0UL));
    }

    /// <summary>A second transfer while one is held is refused as busy and returned, not thrown; an abort frees the group for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyRefusalIsReturnedAbortFrees(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var image = TransferFixtures.Image();
        var first = await scope.Adapter.TransferReplicaState(BeginRequest(scope, image), scope.Context);
        var second = BeginRequest(scope, image);
        second.TransferId = "transfer-2";

        var busy = await scope.Adapter.TransferReplicaState(second, scope.Context);
        var abort = await scope.Adapter.TransferReplicaState(Request(scope, 0U, static r => r.Abort = new ReplicaTransferAbort()), scope.Context);
        var again = await scope.Adapter.TransferReplicaState(second, scope.Context);

        _ = await Assert.That((first.Success, busy.Success, busy.RefusalCode)).IsEqualTo((true, false, RefusalCodes.TransferBusy));
        _ = await Assert.That((abort.Success, again.Success)).IsEqualTo((true, true));
    }

    /// <summary>Values that do not fit the receiver are refused as unusable without reaching it and without throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutOfRangeValuesAreRefused(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var begin = BeginRequest(scope, TransferFixtures.Image());
        begin.Begin.SnapshotBytes = uint.MaxValue;
        var wipe = Request(scope, uint.MaxValue, static r => r.Wipe = new ReplicaTransferWipe());
        var snapshot = Request(scope, 1U, static r => r.Snapshot = new ReplicaTransferSnapshot { Offset = ulong.MaxValue });
        var end = Request(scope, uint.MaxValue, static r => r.End = new WireEnd());
        var batch = Request(scope, uint.MaxValue, static r => r.Entries = new ReplicaTransferEntries());

        var replies = new[]
        {
            await scope.Adapter.TransferReplicaState(begin, scope.Context),
            await scope.Adapter.TransferReplicaState(wipe, scope.Context),
            await scope.Adapter.TransferReplicaState(snapshot, scope.Context),
            await scope.Adapter.TransferReplicaState(end, scope.Context),
            await scope.Adapter.TransferReplicaState(batch, scope.Context),
        };

        _ = await Assert.That(Array.TrueForAll(replies, static r => !r.Success && string.Equals(r.RefusalCode, RefusalCodes.ChecksumMismatch, StringComparison.Ordinal) && r.Term == 0UL)).IsTrue();
        _ = await Assert.That(scope.Sink.Calls.Count).IsEqualTo(0);
    }

    /// <summary>A request that carries no step is rejected as malformed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequestWithoutStepIsRejected(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(scope.Adapter.TransferReplicaState(Request(scope, 0U, static _ => { }), scope.Context));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>A sender that does not match the peer certificate is rejected as unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SenderThatDoesNotMatchCertIsRejected(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var request = BeginRequest(scope, TransferFixtures.Image());
        request.Header.SenderNodeId = "n9";
        request.Header.LeaderNodeId = "n9";

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(scope.Adapter.TransferReplicaState(request, scope.Context));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>A sender that names another node as the leader is rejected as forbidden.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SenderThatIsNotLeaderIsRejected(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken);
        var request = BeginRequest(scope, TransferFixtures.Image());
        request.Header.LeaderNodeId = "n9";

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(scope.Adapter.TransferReplicaState(request, scope.Context));

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.PermissionDenied);
    }

    /// <summary>An adapter without a receiver refuses a transfer message as not ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdapterWithoutReceiverRefusesAsNotReady(CancellationToken cancellationToken)
    {
        await using var scope = await CreateScopeAsync(cancellationToken, withReceiver: false);

        var reply = await scope.Adapter.TransferReplicaState(BeginRequest(scope, TransferFixtures.Image()), scope.Context);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.NotReady));
    }

    private static TransferReplicaStateRequest BeginRequest(Scope scope, byte[] image) => Request(
        scope,
        0U,
        r => r.Begin = new ReplicaTransferBegin
        {
            Index = TransferFixtures.SnapshotIndex,
            IndexTerm = TransferFixtures.SnapshotTerm,
            SnapshotBytes = uint.CreateChecked(image.Length),
            SnapshotChecksum = TransferFixtures.ChecksumOf(image),
        });

    private static TransferReplicaStateRequest Request(Scope scope, uint sequence, Action<TransferReplicaStateRequest> step)
    {
        var request = new TransferReplicaStateRequest { Header = scope.Header.Clone(), TransferId = Transfer, Sequence = sequence };
        step(request);
        return request;
    }

    private static ReplicaTransferEntry WireEntry(string cacheName, string key, long expiresUtcTicks) => new()
    {
        CacheName = cacheName,
        Key = ByteString.CopyFromUtf8(key),
        Payload = ByteString.CopyFrom(1, 2),
        ExpiresUtcTicks = expiresUtcTicks,
    };

    /// <summary>Sends a whole transfer: begin, two snapshot chunks, wipe steps until the group is empty, one batch of two entries and the end.</summary>
    /// <param name="scope">The adapter under test.</param>
    /// <returns>The reply to every message, in order.</returns>
    private static async Task<TransferReplicaStateResponse[]> RunAsync(Scope scope)
    {
        var image = TransferFixtures.Image();
        var half = image.Length / 2;
        var replies = new List<TransferReplicaStateResponse>
        {
            await scope.Adapter.TransferReplicaState(BeginRequest(scope, image), scope.Context),
            await scope.Adapter.TransferReplicaState(
                Request(scope, 1U, r => r.Snapshot = new ReplicaTransferSnapshot { Offset = 0UL, Chunk = ByteString.CopyFrom(image, 0, half) }),
                scope.Context),
            await scope.Adapter.TransferReplicaState(
                Request(scope, 2U, r => r.Snapshot = new ReplicaTransferSnapshot { Offset = ulong.CreateChecked(half), Chunk = ByteString.CopyFrom(image, half, image.Length - half) }),
                scope.Context),
        };
        var sequence = 3U;
        do
            replies.Add(await scope.Adapter.TransferReplicaState(Request(scope, sequence++, static r => r.Wipe = new ReplicaTransferWipe()), scope.Context));
        while (replies[^1].More);

        var batch = new ReplicaTransferEntries();
        batch.Entries.Add(WireEntry("cache", "k-1", 0L));
        batch.Entries.Add(WireEntry("other", "k-2", 123456L));
        replies.Add(await scope.Adapter.TransferReplicaState(Request(scope, sequence++, r => r.Entries = batch), scope.Context));
        replies.Add(await scope.Adapter.TransferReplicaState(Request(scope, sequence, static r => r.End = new WireEnd { EntryCount = 2UL, EntriesCrc32C = 0U }), scope.Context));
        return [.. replies];
    }

    private static async Task<Scope> CreateScopeAsync(CancellationToken cancellationToken, ReplicaTransferOptions? options = null, bool withReceiver = true)
    {
        var dir = new TempDirectory("squirix-transfer-adapter");
        await TransferFixtures.SeedBehindAsync(dir, cancellationToken);
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var logOptions = new FollowerLogOptions { TimeProvider = clock, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, TransferFixtures.Fingerprint, 1UL, NullLoggerFactory.Instance, logOptions) { ElectionClock = clock };
        await registry.OpenAsync(cancellationToken);
        var sink = new CountingSink();
        var members = RocksDoubles.CreateReplicaMembers();
        var receiver = new ReplicaTransferReceiver(registry, members, sink, options ?? new ReplicaTransferOptions(), clock, NullLogger<ReplicaTransferReceiver>.Instance);
        var mtls = new MtlsOptions { InternalListenPort = 6001 };
        var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var certificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, LeaderId);
        var material = MtlsCertificate.Create(certificate, bundle.Ca);
        var topology = new TopologyOptions(new ServerPeer { NodeId = LeaderId, Uri = new Uri("https://localhost:6002") });
        var adapter = new SquirixReplicationServiceAdapter(topology, mtls, material, TimeProvider.System, registry, members, withReceiver ? receiver : null);
        var httpContext = new DefaultHttpContext { Connection = { LocalPort = mtls.InternalListenPort, ClientCertificate = certificate } };
        var header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            GroupId = GroupId,
            TopologyFingerprint = ByteString.CopyFrom(TransferFixtures.Fingerprint),
            ConfigurationGeneration = 1UL,
            Term = 1UL,
            LeaderNodeId = LeaderId,
            SenderNodeId = LeaderId,
        };
        return new Scope
        {
            Adapter = adapter,
            Bundle = bundle,
            Certificate = certificate,
            Clock = clock,
            Context = new TestServerCallContext(null, httpContext),
            Dir = dir,
            Header = header,
            Material = material,
            Receiver = receiver,
            Registry = registry,
            Sink = sink,
        };
    }

    /// <summary>A state sink that holds a scripted number of keys and keeps what it is asked to do, in order.</summary>
    private sealed class CountingSink : IReplicaStateSink
    {
        internal List<string> Calls { get; } = [];

        internal int Keys { get; set; }

        internal ulong? ResetIndex { get; private set; }

        internal List<ReplicaStateEntry> Written { get; } = [];

        public ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken)
        {
            Calls.Add("enter");
            return ValueTask.FromResult(true);
        }

        public void Leave(string groupId) => Calls.Add("leave");

        public void ResetAppliedTo(string groupId, ulong index)
        {
            Calls.Add("reset");
            ResetIndex = index;
        }

        public ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken)
        {
            Calls.Add("seal");
            return ValueTask.FromResult(new ReplicaStateTotals(ulong.CreateChecked(Written.Count), 0U));
        }

        public ValueTask<bool> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken)
        {
            Calls.Add("wipe");
            var examined = Math.Min(maxExamined, Keys);
            Keys -= examined;
            return ValueTask.FromResult(examined == maxExamined);
        }

        public ValueTask WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken)
        {
            Calls.Add("write");
            Written.AddRange(entries.Span);
            return ValueTask.CompletedTask;
        }
    }

    [Immutable]
    private sealed class Scope : IAsyncDisposable
    {
        internal required SquirixReplicationServiceAdapter Adapter { get; init; }

        internal required MtlsTestCertificateBundle Bundle { get; init; }

        internal required X509Certificate2 Certificate { get; init; }

        internal required FakeTimeProvider Clock { get; init; }

        internal required TestServerCallContext Context { get; init; }

        internal required TempDirectory Dir { get; init; }

        internal required ReplicationEnvelopeHeader Header { get; init; }

        internal required IDisposable Material { get; init; }

        internal required ReplicaTransferReceiver Receiver { get; init; }

        internal required ReplicaGroupRegistry Registry { get; init; }

        internal required CountingSink Sink { get; init; }

        public async ValueTask DisposeAsync()
        {
            Receiver.Dispose();
            await Registry.DisposeAsync().ConfigureAwait(false);
            Dir.Dispose();
            Material.Dispose();
            Certificate.Dispose();
            Bundle.Dispose();
        }
    }
}
