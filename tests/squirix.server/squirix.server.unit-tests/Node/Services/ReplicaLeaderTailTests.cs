using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=3 group owner restarted with an uncommitted log tail recovers it and regains its write quorum.</summary>
public sealed class ReplicaLeaderTailTests : ServerUnitTestBase
{
    private const int OlderTermTailEventId = 4010;

    /// <summary>A tail the followers already hold commits and is applied at the first verification after the restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailOnMajorityCommitsAtStart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-majority");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);

        var outcome = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaVerification.AllReady);
        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 2UL));
        await SequenceAssert.EqualAsync(["k1"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>Followers that hold the commit position but not the tail are re-sent the tail, verified, and the tail commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailOnNoFollowerIsRedriven(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-redrive");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 1);
        gateway.Set("n3", FollowerMode.Behind, 1);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache);

        var outcome = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That(gateway.Appends).Contains(("n2", 1UL, 1));
        _ = await Assert.That(gateway.Appends).Contains(("n3", 1UL, 1));
        await SequenceAssert.EqualAsync(["k1"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// With every follower down the tail stays uncommitted and unapplied, and writes are refused before any append; once a follower is
    /// back, verification admits it into the running coordinator, the tail commits, and writes continue after it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailWithFollowersDownStaysPending(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-down");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Pending);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));
        var pending = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((pending.LastLogIndex, pending.CommitIndex)).IsEqualTo((2UL, 1UL));
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();

        gateway.Set("n2", FollowerMode.Match);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Pending);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).CommitIndex).IsEqualTo(2UL);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);

        await SequenceAssert.EqualAsync(["k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A same-identity retry of a tail entry after its commit replays the recorded outcome instead of re-executing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailRetryReturnsOutcomeAfterCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-retry-after");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.AllReady);

        var added = await committer.CommitTryAddAsync(TailOperationId("k1"), "cache", "k1", Entry("k1"), cancellationToken);

        _ = await Assert.That(added).IsTrue();
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        await SequenceAssert.EqualAsync(["k1"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A replayed retry of a committed tail entry leaves the log dense: the next new write commits right after the tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailReplayThenNewWriteSucceeds(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-replay");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That(await committer.CommitTryAddAsync(TailOperationId("k1"), "cache", "k1", Entry("k1"), cancellationToken)).IsTrue();

        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((3UL, 3UL));
        await SequenceAssert.EqualAsync(["k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A same-identity retry of a tail entry that is not committed yet reports the unknown outcome on every attempt, never a definite
    /// failure and never a re-execution.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailRetryBeforeCommitIsUnknown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-retry-before");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var retry = committer.CommitTryAddAsync(TailOperationId("k1"), "cache", "k1", Entry("k1"), cancellationToken);
            var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(retry);
            _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        }

        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>The tail recovered at the first write after a restart is applied in log order before that write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TailAppliedInOrderBeforeNewWrite(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-order");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1", "k2");
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((4UL, 4UL));
        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A tail holding no entry of the leader's current term is not committed by counting replicas, even when every follower holds it:
    /// verification reports blocked with a warning, and writes stay refused until a current-term entry could commit it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OlderTermTailIsBlockedUntilNoOp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-older-term");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 2, cancellationToken, "k1");
        var cache = new StubCache();
        var log = new EventRecordingLogger();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache, log);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(log.Find(OlderTermTailEventId)?.Level).IsEqualTo(LogLevel.Warning);
        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 1UL));
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>A follower that accepts the re-sent tail but reports a longer log than the leader is held back instead of counting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LongerFollowerNotReadyAfterRedrive(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-longer");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.BehindLonger);
        gateway.Set("n3", FollowerMode.Down);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache);

        var outcome = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(ReplicaVerification.Pending);
        _ = await Assert.That(gateway.Appends).Contains(("n2", 1UL, 1));
        _ = await Assert.That(registry.EligibilityFor("n1").StateFor(1)).IsEqualTo(ReplicaParticipantState.CatchingUp);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>
    /// A commit that advances the leader log while the coordinator starts, as one a disposed coordinator left running does, does not
    /// fault verification: the start reads the status and the tail as one view, so the tail never falls short of the status.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The owned group log is not open.</exception>
    [Test]
    public async Task VerifyToleratesMovingTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-tail-moving");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 1, cancellationToken, "k1", "k2");
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(dir, new FollowerLogOptions { FaultHooks = hooks }, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        // From the first probe on, a stalled commit holds the log gate, so the coordinator start queues its first log read behind it.
        Task<FollowerLogCommitResult>? stalled = null;
        var gateway = new FirstCallGateway(
            new ScriptedGateway(),
            () =>
            {
                hooks.StallNextMetaWrite();
                stalled = log.AdvanceCommitAsync(2, cancellationToken);
            });
        await using var committer = CreateCommitter(registry, gateway);

        var verify = committer.VerifyReplicasAsync(cancellationToken);
        _ = await Assert.That(verify.IsCompleted).IsFalse();

        // Queued after the start's first log read and ahead of any later one: the tail moves right after that read.
        var moved = log.AdvanceCommitAsync(3, cancellationToken);
        await hooks.Entered;
        hooks.Release();

        _ = await Assert.That((await stalled!).Success).IsTrue();
        _ = await Assert.That((await moved).Success).IsTrue();
        _ = await Assert.That(await verify).IsEqualTo(ReplicaVerification.AllReady);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).CommitIndex).IsEqualTo(3UL);
    }

    /// <summary>Follower double that runs a scripted action on the first request, before answering it like the wrapped gateway.</summary>
    private sealed class FirstCallGateway : IReplicaRpcGateway
    {
        private readonly IReplicaRpcGateway _inner;
        private Action? _onFirstCall;

        internal FirstCallGateway(IReplicaRpcGateway inner, Action onFirstCall)
        {
            _inner = inner;
            _onFirstCall = onFirstCall;
        }

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            Interlocked.Exchange(ref _onFirstCall, null)?.Invoke();
            return _inner.AppendEntriesAsync(nodeId, header, batch, cancellationToken);
        }
    }
}
