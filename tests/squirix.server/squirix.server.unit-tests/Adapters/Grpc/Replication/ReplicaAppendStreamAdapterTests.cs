using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationFollowerAdapterTests;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Checks frame ordering and repeated validation against a real follower log.</summary>
public sealed class ReplicaAppendStreamAdapterTests : ServerUnitTestBase
{
    /// <summary>Every heartbeat receives its own numbered log response.</summary>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    public async Task FramesGetFreshReplies(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var first = Frame(1, follower.Header);
        var second = Frame(2, follower.Header.Clone());
        second.Request.Header.Term++;
        var output = new Replies();

        await follower.Adapter.AppendReplicaEntriesStreamAsync(new Requests([first, second]), output, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That(output.Values.Count).IsEqualTo(2);
        _ = await Assert.That((output.Values[0].Sequence, output.Values[0].Response.Term, output.Values[0].Response.Success)).IsEqualTo((1UL, 7UL, true));
        _ = await Assert.That((output.Values[1].Sequence, output.Values[1].Response.Term, output.Values[1].Response.Success)).IsEqualTo((2UL, 8UL, true));
        var durable = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(durable.CurrentTerm).IsEqualTo(8UL);
    }

    /// <summary>Missing, duplicate and skipped sequence numbers never receive a log acknowledgement.</summary>
    /// <param name="sequence">The sequence for this test.</param>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    [Arguments(0UL)]
    [Arguments(1UL)]
    [Arguments(3UL)]
    public async Task BadSequenceStopsStream(ulong sequence, CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var output = new Replies();
        var frames = new Requests([Frame(1, follower.Header), Frame(sequence, follower.Header)]);

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.AppendReplicaEntriesStreamAsync(frames, output, new TestServerCallContext(null, follower.HttpContext)), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(output.Values.Count).IsEqualTo(1);
    }

    /// <summary>An authenticated first frame does not authorize a changed sender in the next frame.</summary>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    public async Task EveryFrameChecksSender(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var invalid = Frame(2, follower.Header.Clone());
        invalid.Request.Header.SenderNodeId = "node-b";
        var output = new Replies();

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.AppendReplicaEntriesStreamAsync(new Requests([Frame(1, follower.Header), invalid]), output, new TestServerCallContext(null, follower.HttpContext)), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
        _ = await Assert.That(output.Values.Count).IsEqualTo(1);
    }

    /// <summary>Headers are revalidated even after the first frame succeeded.</summary>
    /// <param name="kind">The invalid header field.</param>
    /// <param name="expected">The expected refusal status.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0, StatusCode.InvalidArgument)]
    [Arguments(1, StatusCode.PermissionDenied)]
    [Arguments(2, StatusCode.InvalidArgument)]
    public async Task EveryFrameChecksHeader(int kind, StatusCode expected, CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var invalid = Frame(2, follower.Header.Clone());
        switch (kind)
        {
            case 0: invalid.Request.Header.SchemaVersion++; break;
            case 1: invalid.Request.Header.LeaderNodeId = "node-b"; break;
            default: invalid.Request.Header = null; break;
        }

        var output = new Replies();
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.AppendReplicaEntriesStreamAsync(new Requests([Frame(1, follower.Header), invalid]), output, new TestServerCallContext(null, follower.HttpContext)), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(expected);
        _ = await Assert.That(output.Values.Count).IsEqualTo(1);
    }

    /// <summary>A numbered append stores its canonical bytes before acknowledging the entry index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StreamStoresCanonicalEntry(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var record = new ReplicaLogRecord(1, 7, "op", "cache", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 1 }, "Set", new byte[] { 2 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0);
        var payload = ReplicaLogCodec.Encode(in record);
        var frame = Frame(1, follower.Header);
        frame.Request.Entries.Add(new ReplicaLogEntry { LogIndex = 1, Term = 7, CanonicalPayload = ByteString.CopyFrom(payload) });
        var output = new Replies();

        await follower.Adapter.AppendReplicaEntriesStreamAsync(new Requests([frame]), output, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That((output.Values[0].Response.Success, output.Values[0].Response.LastLogIndex)).IsEqualTo((true, 1UL));
        var tail = await GroupLog(follower).GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(1);
        _ = await Assert.That(tail[0].Payload.Span.SequenceEqual(payload)).IsTrue();
    }

    /// <summary>A missing inner message is rejected before the follower log is entered.</summary>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    public async Task MissingRequestIsRefused(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var output = new Replies();

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(follower.Adapter.AppendReplicaEntriesStreamAsync(new Requests([new ReplicaAppendStreamRequest { Sequence = 1 }]), output, new TestServerCallContext(null, follower.HttpContext)), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(output.Values.Count).IsEqualTo(0);
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(0UL);
    }

    /// <summary>Host shutdown ends an idle inbound stream without waiting for a remote peer to disconnect.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostStopEndsIdleInboundCall(CancellationToken cancellationToken)
    {
        using var stopping = new CancellationTokenSource();
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken, stopping.Token);
        var input = new IdleRequests();
        var output = new Replies();
        var call = follower.Adapter.AppendReplicaEntriesStreamAsync(input, output, new TestServerCallContext(null, follower.HttpContext));
        await input.Arrived.Task.WaitAsync(cancellationToken);

        await stopping.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(call, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(output.Values.Count).IsEqualTo(0);
    }

    private static ReplicaAppendStreamRequest Frame(ulong sequence, ReplicationEnvelopeHeader header) =>
        new() { Sequence = sequence, Request = new AppendReplicaEntriesRequest { Header = header } };

    private sealed class IdleRequests : IAsyncStreamReader<ReplicaAppendStreamRequest>
    {
        public ReplicaAppendStreamRequest Current => throw new NotSupportedException("An idle stream yields no frames.");

        internal TaskCompletionSource Arrived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            _ = Arrived.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken);
            return false;
        }
    }

    private sealed class Requests : IAsyncStreamReader<ReplicaAppendStreamRequest>
    {
        private readonly ReplicaAppendStreamRequest[] _frames;
        private int _index = -1;

        internal Requests(ReplicaAppendStreamRequest[] frames)
        {
            _frames = frames;
        }

        public ReplicaAppendStreamRequest Current => _frames[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++_index < _frames.Length);
    }

    private sealed class Replies : IServerStreamWriter<ReplicaAppendStreamResponse>
    {
        public WriteOptions? WriteOptions { get; set; }

        internal List<ReplicaAppendStreamResponse> Values { get; } = [];

        public Task WriteAsync(ReplicaAppendStreamResponse message)
        {
            Values.Add(message);
            return Task.CompletedTask;
        }
    }
}
