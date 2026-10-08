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

    private static FollowerBatch Batch() => new([], PeerNodeId, 1, 0, 0, 0);

    private static ServerClientPool CreatePool(HttpMessageHandler handler, Meter meter)
    {
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new IdleCallPolicy(),
            OwnedHandlerFactory = (_, _, _) => handler,
        };
        var peer = new ServerPeer { NodeId = PeerNodeId, Uri = new Uri("https://localhost:6500") };
        return new ServerClientPool([peer], args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
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
