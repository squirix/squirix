using System;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Validation and pool-lifetime tests for <see cref="ReplicaRpcGateway" />.</summary>
[Immutable]
public sealed class ReplicaRpcGatewayTests : ServerUnitTestBase
{
    private const string PeerNodeId = "n0";

    /// <summary>Verifies that the gateway requires a client pool.</summary>
    [Test]
    public void GatewayRequiresPool()
    {
        IServerClientPool? pool = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(pool, static p => _ = new ReplicaRpcGateway(p!));
    }

    /// <summary>The memory pressure flag of a reply reaches the outcome of the leader; a reply that omits it, as one of an older node, reads as no pressure.</summary>
    [Test]
    public async Task AppendReplyCarriesMemoryPressure()
    {
        var flagged = ReplicaRpcGateway.MapAppendResponse(new AppendReplicaEntriesResponse { Success = true, Term = 3, LastLogIndex = 9, MemoryPressure = true });
        var plain = ReplicaRpcGateway.MapAppendResponse(new AppendReplicaEntriesResponse { Success = true, Term = 3, LastLogIndex = 9 });

        _ = await Assert.That((flagged.Success, flagged.CurrentTerm, flagged.LastLogIndex, flagged.MemoryPressure)).IsEqualTo((true, 3UL, 9UL, true));
        _ = await Assert.That(plain.MemoryPressure).IsFalse();
    }

    /// <summary>The request of a batch carries each entry as its position and the bytes of the entry, which are the very buffer of the entry.</summary>
    [Test]
    public async Task RequestCarriesCanonicalBytes()
    {
        byte[] first = [1, 2, 3];
        byte[] second = [4, 5];
        var batch = new FollowerBatch([new FollowerLogEntry(5, 2, first), new FollowerLogEntry(6, 3, second)], PeerNodeId, 3, 4, 2, 5);
        var header = Header();

        var request = ReplicaRpcGateway.MapRequest(in header, in batch);

        _ = await Assert.That(request.Entries.Count).IsEqualTo(2);
        _ = await Assert.That((request.Entries[0].LogIndex, request.Entries[0].Term)).IsEqualTo((5UL, 2UL));
        _ = await Assert.That((request.Entries[1].LogIndex, request.Entries[1].Term)).IsEqualTo((6UL, 3UL));
        _ = await Assert.That(request.Entries[0].CanonicalPayload.Memory.Equals(first)).IsTrue().Because("The payload must be wrapped, not copied.");
        _ = await Assert.That(request.Entries[1].CanonicalPayload.Memory.Equals(second)).IsTrue().Because("The payload must be wrapped, not copied.");
        _ = await Assert.That((request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommitIndex)).IsEqualTo((4UL, 2UL, 5UL));
    }

    /// <summary>The header of an append and of a vote carries the bytes of the topology fingerprint, wrapped instead of copied.</summary>
    [Test]
    public async Task RequestHeaderWrapsTopologyFingerprint()
    {
        byte[] fingerprint = [9, 8, 7, 6];
        var header = Header() with { TopologyFingerprint = fingerprint };

        var mapped = ReplicaRpcGateway.MapHeader(in header);
        var request = ReplicaRpcGateway.MapRequest(in header, Batch());

        await SequenceAssert.EqualAsync(fingerprint, mapped.TopologyFingerprint.ToByteArray());
        await SequenceAssert.EqualAsync(fingerprint, request.Header.TopologyFingerprint.ToByteArray());
        _ = await Assert.That(request.Header.TopologyFingerprint.Memory.Equals(fingerprint)).IsTrue().Because("The fingerprint must be wrapped, not copied.");
    }

    /// <summary>Disposing the pool cancels a replication call in flight, which fails, and disposes the call's handler only after that.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PoolDisposeCancelsInFlightCall(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var parking = new ParkingHandler();
        var pool = CreatePool(parking, meter);
        var gateway = new ReplicaRpcGateway(pool);
        var call = gateway.AppendEntriesAsync(PeerNodeId, Header(), Batch(), cancellationToken);
        await parking.Arrived.WaitAsync(cancellationToken);

        await pool.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(call);
        await parking.Cancelled.WaitAsync(cancellationToken);
        _ = await Assert.That(parking.Disposed).IsTrue();
        _ = await Assert.That(parking.DisposedAfterCancel).IsTrue().Because("The handler must outlive the call it was cancelled under.");
    }

    /// <summary>A call issued after the pool started to dispose is refused without reaching the transport.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallAfterPoolDisposeIsRefused(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var parking = new ParkingHandler();
        var pool = CreatePool(parking, meter);
        var gateway = new ReplicaRpcGateway(pool);
        await pool.DisposeAsync();

        var call = gateway.AppendEntriesAsync(PeerNodeId, Header(), Batch(), cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(call);
        _ = await Assert.That(parking.Arrived.IsCompleted).IsFalse();
    }

    /// <summary>Disposing the pool fails a vote in flight instead of reporting an answer, and disposes the handler only after that.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PoolDisposeCancelsInFlightVote(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var parking = new ParkingHandler();
        var pool = CreatePool(parking, meter);
        var gateway = new ReplicaRpcGateway(pool);
        var call = gateway.RequestVoteAsync(PeerNodeId, Header(), 0, 0, cancellationToken);
        await parking.Arrived.WaitAsync(cancellationToken);

        await pool.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(call);
        await parking.Cancelled.WaitAsync(cancellationToken);
        _ = await Assert.That(parking.DisposedAfterCancel).IsTrue().Because("The handler must outlive the call it was cancelled under.");
    }

    /// <summary>Vote and pre-vote calls issued after the pool started to dispose are refused without reaching the transport.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteCallAfterPoolDisposeIsRefused(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var parking = new ParkingHandler();
        var pool = CreatePool(parking, meter);
        var gateway = new ReplicaRpcGateway(pool);
        await pool.DisposeAsync();

        var preVote = gateway.PreVoteAsync(PeerNodeId, Header(), 0, 0, cancellationToken);
        var vote = gateway.RequestVoteAsync(PeerNodeId, Header(), 0, 0, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(preVote);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(vote);
        _ = await Assert.That(parking.Arrived.IsCompleted).IsFalse();
    }

    /// <summary>A vote whose header carries no leader identity still reaches the transport, because none is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteWithoutLeaderIdReachesTransport(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var parking = new ParkingHandler();
        var pool = CreatePool(parking, meter);
        var gateway = new ReplicaRpcGateway(pool);

        // A default header leaves the leader identity null, as an unset field of a caller would.
        var header = default(ReplicaRpcHeader) with { GroupId = PeerNodeId, ConfigurationGeneration = 1, Term = 2, SenderNodeId = PeerNodeId };

        var call = gateway.RequestVoteAsync(PeerNodeId, header, 0, 0, cancellationToken);
        var first = await Task.WhenAny(call, parking.Arrived).WaitAsync(cancellationToken);

        _ = await Assert.That(first).IsSameReferenceAs(parking.Arrived).Because("The vote must reach the transport instead of failing while it is built.");
        await pool.DisposeAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(call);
    }

    private static FollowerBatch Batch() => new([], PeerNodeId, 1, 0, 0, 0);

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

    private static ReplicaRpcHeader Header() => new(PeerNodeId, ReadOnlyMemory<byte>.Empty, 1, 1, PeerNodeId, PeerNodeId);

    /// <summary>Parks every request until its token is cancelled, then fails it as the transport would.</summary>
    private sealed class ParkingHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource _arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource _cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a task that completes once a request reached the handler.</summary>
        internal Task Arrived => _arrived.Task;

        /// <summary>Gets a task that completes once the parked request observed its cancellation.</summary>
        internal Task Cancelled => _cancelled.Task;

        internal bool Disposed { get; private set; }

        /// <summary>Gets a value indicating whether the parked request had already observed its cancellation when the handler was disposed.</summary>
        internal bool DisposedAfterCancel { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposedAfterCancel = _cancelled.Task.IsCompleted;
                Disposed = true;
            }

            base.Dispose(disposing);
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _ = _arrived.TrySetResult();
            var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (cancellationToken.UnsafeRegister(static state => Release(state), released))
                await released.Task.ConfigureAwait(false);

            _ = _cancelled.TrySetResult();
            throw new OperationCanceledException(cancellationToken);
        }

        private static void Release(object? state)
        {
            if (state is TaskCompletionSource released)
                _ = released.TrySetResult();
        }
    }
}
