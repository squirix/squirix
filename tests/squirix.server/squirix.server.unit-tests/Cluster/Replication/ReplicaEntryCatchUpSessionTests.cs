using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Entry catch-up of one follower from the leader log through a paused follower sender.</summary>
[Immutable]
public sealed class ReplicaEntryCatchUpSessionTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A follower missing the leader's suffix gets it in one round after the first mismatch and is then confirmed at the target.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingSuffixIsSentThenConfirmed(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-suffix");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 6, 1, cancellationToken);
        _ = await leader.AdvanceCommitAsync(4, cancellationToken);
        await AppendAsync(follower, 1, 2, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.CaughtUp, 6, 1, 0, 4, 3));
        var status = await follower.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((6UL, 4UL));
        _ = await Assert.That(gateway.Arrivals.Count).IsEqualTo(1);
        _ = await Assert.That(gateway.Arrivals.TryPeek(out var batch) && batch == ("n2", 3UL, 6UL, 2UL)).IsTrue();
    }

    /// <summary>The first mismatch reports the follower's last index, and the next request starts right after it instead of walking back one by one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JumpsToFollowerLastIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-jump");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 40, 1, cancellationToken);
        await AppendAsync(follower, 1, 2, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken, 16);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.CaughtUp, 40, 1, 0, 38, 5));
        _ = await Assert.That(gateway.Arrivals.TryDequeue(out var first) && first == ("n2", 3UL, 18UL, 2UL)).IsTrue();
        _ = await Assert.That(gateway.Arrivals.TryDequeue(out var second) && second == ("n2", 19UL, 34UL, 18UL)).IsTrue();
        _ = await Assert.That(gateway.Arrivals.TryDequeue(out var third) && third == ("n2", 35UL, 40UL, 34UL)).IsTrue();
        _ = await Assert.That((await follower.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(40UL);
    }

    /// <summary>A divergent uncommitted follower tail is backed over, truncated, and replaced by the leader's entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DivergentTailIsReplaced(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-divergent");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 1, 1, cancellationToken);
        await AppendAsync(leader, 2, 3, 2, cancellationToken);
        await AppendAsync(follower, 1, 1, 1, cancellationToken);
        await AppendAsync(follower, 2, 2, 1, cancellationToken, "divergent");
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 2, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.CaughtUp, 3, 2, 0, 2, 4));
        var held = await follower.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(held.Count).IsEqualTo(3);
        _ = await Assert.That((held[1].LogIndex, held[1].Term)).IsEqualTo((2UL, 2UL));
        _ = await Assert.That(Encoding.UTF8.GetString(ReplicaLogCodec.Decode(held[1].Payload).GetValueOrDefault().MutationPayload.Span)).IsEqualTo("v");
    }

    /// <summary>The leader's uncommitted tail is sent like any other entry, and the follower commit follows the leader's.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UncommittedLeaderTailIsSent(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-tail");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 3, 1, cancellationToken);
        _ = await leader.AdvanceCommitAsync(1, cancellationToken);
        await AppendAsync(follower, 1, 1, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That((result.Outcome, result.HeldThrough, result.EntriesSent)).IsEqualTo((ReplicaCatchUpOutcome.CaughtUp, 3UL, 2));
        var status = await follower.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((3UL, 1UL));
    }

    /// <summary>A follower behind the leader's compacted prefix is not sent anything: the session fails closed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedPrefixFailsClosed(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-compacted");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 6, 1, cancellationToken);
        _ = await leader.AdvanceCommitAsync(5, cancellationToken);
        _ = await leader.AdvanceAppliedAsync(5, cancellationToken);
        _ = await Assert.That(await leader.CompactThroughAsync(5, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        await AppendAsync(follower, 1, 2, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Compacted, 0, 0, 0, 0, 1));
        _ = await Assert.That(gateway.Arrivals).IsEmpty();
        _ = await Assert.That((await follower.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A request stops before the entry that would take it past the byte cap; a single larger entry goes out alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ByteCapSplitsRequests(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-bytes");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 3, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken, maxBatchBytes: 1);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.CaughtUp, 3, 1, 0, 3, 5));
        _ = await Assert.That(gateway.Arrivals.Count).IsEqualTo(3);
        _ = await Assert.That(gateway.Arrivals.TryDequeue(out var first) && first == ("n2", 1UL, 1UL, 0UL)).IsTrue();
    }

    /// <summary>A follower with a higher term ends the session with that term, and nothing is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleTermAborts(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-stale");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        await AppendAsync(follower, 1, 1, 5, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.StaleTerm, 0, 0, 5, 0, 1));
        _ = await Assert.That(gateway.Arrivals).IsEmpty();
    }

    /// <summary>A follower whose log is not ready refuses, and the session reports the refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotReadyIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-not-ready");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = new FollowerLog(Path.Join(dir, "follower"), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That((result.Outcome, result.Rounds)).IsEqualTo((ReplicaCatchUpOutcome.Refused, 1));
        _ = await Assert.That(gateway.Refusals.TryPeek(out var refusal) && string.Equals(refusal.Refusal, RefusalCodes.NotReady, StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>A request that fails in transport ends the session as unreachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransportFailureIsUnreachable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-transport");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var run = RunLeasedAsync(sender, leader, cancellationToken);
            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Fail(new IOException("follower is down"));

            var result = await BoundedAsync(run, cancellationToken);

            _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Unreachable, 0, 0, 0, 0, 1));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A leader frame that cannot be read back intact ends the session as corrupt, before anything is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornLeaderFrameIsCorrupt(CancellationToken cancellationToken)
    {
        var status = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 3, 0, FollowerLogReadiness.Ready);
        var expectations = new IFollowerLogCreateExpectations();
        _ = expectations.Setups.GetStatusAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(status));
        _ = expectations.Setups.ReadEntriesAsync(Arg.Any<ulong>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                        .ReturnValue(Task.FromException<FollowerLogEntriesRead>(new InvalidDataException("torn frame")));
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);

            var result = await new ReplicaEntryCatchUpSession(expectations.Instance(), "n1", 1).RunAsync(lease, cancellationToken);

            _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Corrupt, 0, 0, 0, 0, 0));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
        }
        finally
        {
            await sender.DisposeAsync();
        }
    }

    /// <summary>A follower that keeps reporting one index less is followed only up to the back-up cap, then given up as diverged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BackUpsAreCapped(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-back-ups");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await AppendAsync(leader, 1, 6, 1, cancellationToken);
        var expectations = new IReplicaRpcGatewayCreateExpectations();
        _ = expectations.Setups.AppendEntriesAsync(Arg.Any<string>(), Arg.Any<ReplicaRpcHeader>(), Arg.Any<FollowerBatch>(), Arg.Any<CancellationToken>())
                        .Callback(static (_, _, batch, _) => Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, 1, batch.PrevLogIndex - 1)))
                        .ExpectedCallCount(4);
        var sender = CreateSender(expectations.Instance());
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);

            var result = await new ReplicaEntryCatchUpSession(leader, "n1", 1, maxBackUps: 3).RunAsync(lease, cancellationToken);

            _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Diverged, 0, 0, 0, 0, 4));
        }
        finally
        {
            await sender.DisposeAsync();
        }
    }

    /// <summary>A follower that holds unverified entries past the leader's last index is reported as diverged; nothing is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerAheadIsDiverged(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-ahead");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await using var follower = await OpenAsync(Path.Join(dir, "follower"), cancellationToken);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        await AppendAsync(follower, 1, 3, 1, cancellationToken);
        var gateway = Route(follower);

        var result = await RunAsync(leader, gateway, 1, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Diverged, 0, 0, 0, 0, 1));
        _ = await Assert.That(gateway.Arrivals).IsEmpty();
    }

    /// <summary>A session over a lease whose sender closed ends as aborted on its first request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SenderClosedAborts(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-closed");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        using var lease = await sender.BeginCatchUpAsync(cancellationToken);
        await sender.DisposeAsync();

        var result = await new ReplicaEntryCatchUpSession(leader, "n1", 1).RunAsync(lease, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Aborted, 0, 0, 0, 0, 1));
        _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
    }

    /// <summary>Canceling the session token ends the session by throwing, not by an outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationPropagates(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-catch-up-cancel");
        await using var leader = await OpenAsync(Path.Join(dir, "leader"), cancellationToken);
        await AppendAsync(leader, 1, 2, 1, cancellationToken);
        var gateway = new ParkingFollowerGateway { ObservesCancellation = true };
        var sender = CreateSender(gateway);
        try
        {
            using var canceled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var run = RunLeasedAsync(sender, leader, canceled.Token);
            _ = await BoundedAsync(gateway.CallAsync(0), cancellationToken);

            await canceled.CancelAsync();

            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(BoundedAsync(run, cancellationToken));
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Appends entries <paramref name="from" /> through <paramref name="to" /> of one term to a log, each after the entry before it.</summary>
    /// <param name="log">The log.</param>
    /// <param name="from">The first index.</param>
    /// <param name="to">The last index.</param>
    /// <param name="term">The term of the entries and of the request.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="value">The mutation payload of every entry.</param>
    /// <returns>A task that completes once every entry is durable.</returns>
    private static async Task AppendAsync(FollowerLog log, ulong from, ulong to, ulong term, CancellationToken cancellationToken, string value = "v")
    {
        for (var index = from; index <= to; index++)
        {
            var prevTerm = index == 1UL ? 0UL : await log.GetTermAtAsync(index - 1UL, cancellationToken);
            var request = new FollowerLogAppendRequest("n1", term, index - 1UL, prevTerm, 0UL, new ReadOnlyMemory<FollowerLogEntry>([Entry(index, term, value)]));
            _ = await Assert.That((await log.AppendAsync(request, cancellationToken)).Success).IsTrue();
        }
    }

    private static FollowerLogEntry Entry(ulong logIndex, ulong term, string value)
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

    private static async Task<FollowerLog> OpenAsync(string dir, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
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

    /// <summary>Routes the appends sent to n2 into <paramref name="follower" />.</summary>
    /// <param name="follower">The follower log.</param>
    /// <returns>The transport double.</returns>
    private static FollowerLogRoutingGateway Route(FollowerLog follower) =>
        new(new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal) { ["n2"] = follower });

    /// <summary>Takes a catch-up lease on <paramref name="sender" /> and runs one session over it, releasing the lease once the session ended.</summary>
    /// <param name="sender">The follower sender.</param>
    /// <param name="leader">The leader log.</param>
    /// <param name="cancellationToken">The session token.</param>
    /// <returns>The session result.</returns>
    private static async Task<ReplicaCatchUpResult> RunLeasedAsync(ReplicaFollowerSender sender, FollowerLog leader, CancellationToken cancellationToken)
    {
        using var lease = await sender.BeginCatchUpAsync(cancellationToken);
        return await new ReplicaEntryCatchUpSession(leader, "n1", 1).RunAsync(lease, cancellationToken);
    }

    /// <summary>Runs one session for n2 over a fresh sender seeded with the leader's last entry.</summary>
    /// <param name="leader">The leader log.</param>
    /// <param name="gateway">The transport double.</param>
    /// <param name="term">The leader term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="maxBatchEntries">The most entries one request carries.</param>
    /// <param name="maxBatchBytes">The most payload bytes one request carries.</param>
    /// <returns>The session result.</returns>
    private static async Task<ReplicaCatchUpResult> RunAsync(
        FollowerLog leader,
        IReplicaRpcGateway gateway,
        ulong term,
        CancellationToken cancellationToken,
        int maxBatchEntries = 64,
        long maxBatchBytes = 4 * 1024 * 1024)
    {
        var status = await leader.GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1, term, "n1", "n1");
        var sender = new ReplicaFollowerSender(gateway, "n2", in header, status.LastLogIndex, status.LastLogTerm, HangGuard);
        await using (sender)
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var session = new ReplicaEntryCatchUpSession(leader, "n1", term, maxBatchEntries, maxBatchBytes);
            return await BoundedAsync(session.RunAsync(lease, cancellationToken), cancellationToken);
        }
    }
}
