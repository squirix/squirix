using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>The requests <see cref="ReplicaTransferRpcGateway" /> puts on the wire and the replies it maps back, over a scripted transport.</summary>
[Immutable]
public sealed class ReplicaTransferRpcGatewayTests : ServerUnitTestBase
{
    private const string PeerNodeId = "n0";
    private const string Transfer = "transfer-1";

    /// <summary>Verifies that the gateway requires a client pool.</summary>
    [Test]
    public void GatewayRequiresPool()
    {
        IServerClientPool? pool = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(pool, static p => _ = new ReplicaTransferRpcGateway(p!));
    }

    /// <summary>The begin carries the envelope, the transfer id, sequence zero and the baseline with its ceiling flag, and the reply maps back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginMapsEnvelopeAndBaseline(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var transport = new ScriptedHandler { Reply = new TransferReplicaStateResponse { Term = 5UL, Success = false, RefusalCode = RefusalCodes.TransferBusy } };
        var pool = CreatePool(transport, meter);
        var gateway = new ReplicaTransferRpcGateway(pool);
        var baseline = new ReplicaTransferBaseline(7UL, 3UL, 1024, 99U, new byte[] { 1, 2 }, 4UL, true);

        var reply = await gateway.BeginAsync(PeerNodeId, Header(), Transfer, baseline, cancellationToken);
        await pool.DisposeAsync();

        var request = transport.Received[0];
        _ = await Assert.That((request.Header.GroupId, request.Header.SenderNodeId, request.Header.LeaderNodeId, request.Header.Term)).IsEqualTo(("g", "leader", "leader", 9UL));
        _ = await Assert.That(request.Header.ConfigurationGeneration).IsEqualTo(4UL);
        await SequenceAssert.EqualAsync(new byte[] { 1, 2 }, request.Header.TopologyFingerprint.ToByteArray());
        _ = await Assert.That((request.TransferId, request.Sequence, request.StepCase)).IsEqualTo((Transfer, 0U, TransferReplicaStateRequest.StepOneofCase.Begin));
        _ = await Assert.That((request.Begin.Index, request.Begin.IndexTerm, request.Begin.SnapshotBytes, request.Begin.SnapshotChecksum)).IsEqualTo((7UL, 3UL, 1024U, 99U));
        _ = await Assert.That(request.Begin.CeilingChecked).IsTrue();
        _ = await Assert.That((reply.Term, reply.Success, reply.RefusalCode, reply.More)).IsEqualTo((5UL, false, RefusalCodes.TransferBusy, false));
    }

    /// <summary>Every message after the begin and the abort carry their step, sequence number and payload, and the more flag of the reply maps back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MessagesAndAbortMapToTheirSteps(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var transport = new ScriptedHandler { Reply = new TransferReplicaStateResponse { Term = 2UL, Success = true, More = true } };
        var pool = CreatePool(transport, meter);
        var gateway = new ReplicaTransferRpcGateway(pool);
        var entries = new[]
        {
            new ReplicaStateEntry("cache", Encoding.UTF8.GetBytes("k-1"), new byte[] { 1, 2, 3 }, 0L),
            new ReplicaStateEntry("other", Encoding.UTF8.GetBytes("k-2"), new byte[] { 4 }, 123456L),
        };

        var wipe = await gateway.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.Wipe, 3), cancellationToken);
        _ = await gateway.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.Snapshot, 1, 40L, [9, 8]), cancellationToken);
        _ = await gateway.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.Entries, 4, entries: entries), cancellationToken);
        _ = await gateway.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.End, 5, count: 2UL, crc: 77U), cancellationToken);
        _ = await gateway.AbortAsync(PeerNodeId, Header(), Transfer, cancellationToken);
        await pool.DisposeAsync();

        var received = transport.Received;
        _ = await Assert.That((wipe.Term, wipe.Success, wipe.More)).IsEqualTo((2UL, true, true));
        _ = await Assert.That((received[0].StepCase, received[0].Sequence, received[0].Header.GroupId)).IsEqualTo((TransferReplicaStateRequest.StepOneofCase.Wipe, 3U, "g"));
        _ = await Assert.That((received[1].StepCase, received[1].Sequence, received[1].Snapshot.Offset)).IsEqualTo((TransferReplicaStateRequest.StepOneofCase.Snapshot, 1U, 40UL));
        await SequenceAssert.EqualAsync(new byte[] { 9, 8 }, received[1].Snapshot.Chunk.ToByteArray());
        var batch = received[2].Entries.Entries;
        _ = await Assert.That((received[2].StepCase, received[2].Sequence, batch.Count)).IsEqualTo((TransferReplicaStateRequest.StepOneofCase.Entries, 4U, 2));
        _ = await Assert.That((batch[0].CacheName, batch[0].Key.ToStringUtf8(), batch[0].Payload.ToByteArray().Length, batch[0].ExpiresUtcTicks)).IsEqualTo(("cache", "k-1", 3, 0L));
        _ = await Assert.That((batch[1].CacheName, batch[1].Key.ToStringUtf8(), batch[1].Payload.ToByteArray().Length, batch[1].ExpiresUtcTicks)).IsEqualTo(("other", "k-2", 1, 123456L));
        _ = await Assert.That((received[3].StepCase, received[3].Sequence, received[3].End.EntryCount, received[3].End.EntriesCrc32C))
                       .IsEqualTo((TransferReplicaStateRequest.StepOneofCase.End, 5U, 2UL, 77U));
        _ = await Assert.That((received[4].StepCase, received[4].TransferId)).IsEqualTo((TransferReplicaStateRequest.StepOneofCase.Abort, Transfer));
    }

    /// <summary>A negative sequence number or offset is a caller error, thrown before the transport is reached.</summary>
    [Test]
    public async Task NegativeSequenceOrOffsetIsRejected()
    {
        using var meter = new Meter("Squirix");
        using var transport = new ScriptedHandler();
        var pool = CreatePool(transport, meter);
        var gateway = new ReplicaTransferRpcGateway(pool);

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(gateway, static g => _ = g.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.Wipe, -1), CancellationToken.None));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>()
                               .Throws(gateway, static g => _ = g.SendAsync(PeerNodeId, Header(), Message(ReplicaTransferStep.Snapshot, 1, -1L), CancellationToken.None));
        await pool.DisposeAsync();

        _ = await Assert.That(transport.Received.Count).IsEqualTo(0);
    }

    /// <summary>A snapshot chunk of the whole message budget and a batch of the smallest entries counted at the budget both stay within the gRPC message limit.</summary>
    [Test]
    public async Task MessageBudgetFitsTheGrpcLimit()
    {
        var chunk = new byte[ReplicaTransferOptions.MessageBudgetBytes];
        var snapshot = ReplicaTransferWire.MessageRequest(Header(), Message(ReplicaTransferStep.Snapshot, 1, long.MaxValue, chunk));
        var counted = Encoding.UTF8.GetByteCount("c") + 1 + 1 + ReplicaTransferOptions.EntryFramingBytes;
        var small = new ReplicaStateEntry[ReplicaTransferOptions.MessageBudgetBytes / counted];
        Array.Fill(small, new ReplicaStateEntry("c", new byte[] { 1 }, new byte[] { 1 }, long.MaxValue));
        var batch = ReplicaTransferWire.MessageRequest(Header(), Message(ReplicaTransferStep.Entries, int.MaxValue, entries: small));

        _ = await Assert.That(snapshot.CalculateSize()).IsLessThanOrEqualTo(EntryLimits.GrpcMaxSendMessageSizeBytes);
        _ = await Assert.That(batch.CalculateSize()).IsLessThanOrEqualTo(EntryLimits.GrpcMaxSendMessageSizeBytes);
    }

    private static ServerClientPool CreatePool(HttpMessageHandler handler, Meter meter)
    {
        var args = new ServerClientPoolArgs
        {
            TimeProvider = TimeProvider.System,
            PolicyFactory = static _ => new IdleCallPolicy(),
            OwnedHandlerFactory = (_, _, _) => handler,
        };
        var peer = new ServerPeer { NodeId = PeerNodeId, Uri = new Uri("https://localhost:6500") };
        return new ServerClientPool([peer], args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance, NullLogger<PeerHandshakeVerifier>.Instance);
    }

    private static ReplicaRpcHeader Header() => new("g", new byte[] { 1, 2 }, 4UL, 9UL, "leader", "leader");

    private static ReplicaTransferMessage Message(
        ReplicaTransferStep step,
        int sequence,
        long offset = 0L,
        byte[]? chunk = null,
        ReplicaStateEntry[]? entries = null,
        ulong count = 0UL,
        uint crc = 0U)
    {
        return new ReplicaTransferMessage
        {
            Step = step,
            GroupId = "g",
            TransferId = Transfer,
            Sequence = sequence,
            Offset = offset,
            Chunk = chunk,
            Entries = entries,
            EntryCount = count,
            EntriesCrc = crc,
        };
    }

    /// <summary>Answers every call with a scripted gRPC reply and keeps the requests it received.</summary>
    private sealed class ScriptedHandler : HttpMessageHandler
    {
        private const int FrameHeaderBytes = 5;

        internal List<TransferReplicaStateRequest> Received { get; } = [];

        internal TransferReplicaStateResponse Reply { get; init; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            Received.Add(TransferReplicaStateRequest.Parser.ParseFrom(body.AsSpan(FrameHeaderBytes)));
            var payload = Reply.ToByteArray();
            var frame = new byte[FrameHeaderBytes + payload.Length];
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(1), uint.CreateChecked(payload.Length));
            payload.CopyTo(frame, FrameHeaderBytes);
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Version = HttpVersion.Version20, Content = new ByteArrayContent(frame) };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            response.TrailingHeaders.Add("grpc-status", "0");
            return response;
        }
    }
}
