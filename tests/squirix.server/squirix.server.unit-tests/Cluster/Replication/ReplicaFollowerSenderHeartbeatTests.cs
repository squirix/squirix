using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Heartbeats of a <see cref="ReplicaFollowerSender" />: only from an idle slot, never ahead of an entry, every reply observed.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderHeartbeatTests : ServerUnitTestBase
{
    /// <summary>An idle slot sends an empty append at its last entry with the leader commit, and the reply reaches the observer with the slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleSlotHeartbeatsAtLastEntry(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var observer = new ReplyLog(1);
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 5, 1, HangGuard) { ReplicaIndex = 2, ReplyObserver = (slot, reply) => observer.Record(slot, reply.Success) };
        try
        {
            var sent = sender.TryEnqueueHeartbeat(4UL);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            heartbeat.Accept();
            var replies = await BoundedAsync(observer.Completed, cancellationToken);

            _ = await Assert.That(sent).IsTrue();
            _ = await Assert.That((heartbeat.Count, heartbeat.PrevLogIndex, heartbeat.CommitIndex)).IsEqualTo((0, 5UL, 4UL));
            _ = await Assert.That(replies).IsEqualTo("2:True");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>
    /// A slot with a request in flight takes no heartbeat; an entry enqueued behind a heartbeat goes out after it, and both replies are
    /// observed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusySlotSkipsHeartbeat(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var observer = new ReplyLog(2);
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { ReplicaIndex = 1, ReplyObserver = (slot, reply) => observer.Record(slot, reply.Success) };
        try
        {
            var idle = sender.TryEnqueueHeartbeat(0UL);
            var heartbeat = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var entry = EnqueueAsync(sender, 1);
            var busy = sender.TryEnqueueHeartbeat(0UL);
            heartbeat.Accept();
            var append = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            append.Accept();
            _ = await BoundedAsync(entry, cancellationToken);
            var replies = await BoundedAsync(observer.Completed, cancellationToken);

            _ = await Assert.That((idle, busy)).IsEqualTo((true, false));
            _ = await Assert.That((heartbeat.Count, append.FirstIndex)).IsEqualTo((0, 1UL));
            _ = await Assert.That(replies).IsEqualTo("1:True,1:True");
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Collects the observed replies and completes once the expected number arrived.</summary>
    [ThreadSafe]
    private sealed class ReplyLog
    {
        private readonly TaskCompletionSource<string> _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int _expected;
        private readonly List<(int Slot, bool Success)> _replies = [];
        private readonly Lock _sync = new();

        internal ReplyLog(int expected)
        {
            _expected = expected;
        }

        /// <summary>Gets the replies once the expected number arrived, comma-separated as slot:success.</summary>
        internal Task<string> Completed => _completed.Task;

        internal void Record(int slot, bool success)
        {
            lock (_sync)
            {
                _replies.Add((slot, success));
                if (_replies.Count == _expected)
                    _ = _completed.TrySetResult(string.Join(',', _replies.ConvertAll(static reply => $"{reply.Slot}:{reply.Success}")));
            }
        }
    }
}
