using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
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

/// <summary>
/// A committer that leads its own group without an election does not lead a copy that is not complete: its writes and reads are refused with
/// no leader authority, nothing is appended or applied, and its verification reports the group as waiting for its first start. Node n1 owns group n1 over a real log.
/// </summary>
public sealed class ReplicaCommitterUnseededTests : ServerUnitTestBase
{
    /// <summary>The event id of the verification blocked warning.</summary>
    private const int BlockedEventId = 4001;

    private const string CacheName = "cache";

    /// <summary>The event id of a verification that waits for the first start of the group.</summary>
    private const int UnseededEventId = 4063;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A write over an unseeded copy is refused as Unavailable, nothing is appended or applied, and the committer does not start.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededOwnerRefusesWrite(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-write");
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache, NullLogger<ReplicaGroupCommitter>.Instance);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken));
            _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        }

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.LastLogIndex)).IsEqualTo((GroupMemberState.Unseeded, 0UL, 0UL));
        _ = await Assert.That(committer.IsStarted).IsFalse();
        _ = await Assert.That(gateway.Appends.IsEmpty).IsTrue();
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>Verification over an unseeded copy reports the group as waiting for its first start on every pass and probes no follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededOwnerVerificationWaitsForStart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-verify");
        var gateway = new ScriptedGateway();
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Unseeded);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Unseeded);

        _ = await Assert.That(gateway.Appends.IsEmpty).IsTrue();
    }

    /// <summary>The readiness loop reports an unseeded owner as waiting for the first start, as information, and logs no blocked warning.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededOwnerIsReportedWithoutWarning(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-report");
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await using var committers = LedGroupsTestKit.LeadOwn(committer);
        var events = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(committers, events, TimeProvider.System);

        await service.StartAsync(cancellationToken);
        await events.WhenLoggedAsync(UnseededEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await service.StopAsync(cancellationToken);

        _ = await Assert.That(events.Find(UnseededEventId)?.Level).IsEqualTo(LogLevel.Information);
        _ = await Assert.That(events.Count(BlockedEventId)).IsEqualTo(0);
    }

    /// <summary>A read over an unseeded copy, of the entry or of the value, is refused instead of answering from an empty cache, and is served once the copy is complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededOwnerRefusesRead(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-read");
        var cache = new StubCache();
        await cache.SetEntryAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken);
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache, NullLogger<ReplicaGroupCommitter>.Instance);
        await using var committers = LedGroupsTestKit.LeadOwn(committer);
        var replicated = new ReplicatedCache(cache, committers);

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(replicated.GetEntryAsync(CacheName, "k1", cancellationToken));
        var valueRefused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheValueResult<object?>>(replicated.GetValueAsync(CacheName, "k1", cancellationToken));
        _ = registry.TryGetLog("n1", out var log);
        await log!.CompleteBootstrapAsync(cancellationToken);
        var served = await replicated.GetEntryAsync(CacheName, "k1", cancellationToken);

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That(valueRefused.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(served?.Value).IsEqualTo("k1");
    }

    /// <summary>Once the owner completed its copy, the same committer starts, verifies its followers, and commits a write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedOwnerLeadsAtTermOne(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-completed");
        var gateway = new ScriptedGateway();
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        var waiting = await committer.VerifyReplicasAsync(cancellationToken);
        _ = registry.TryGetLog("n1", out var log);
        await log!.CompleteBootstrapAsync(cancellationToken);

        await committer.CommitSetAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That(waiting).IsEqualTo(ReplicaVerification.Unseeded);
        _ = await Assert.That((status.MemberState, status.CurrentTerm, status.CommitIndex)).IsEqualTo((GroupMemberState.Complete, 1UL, 1UL));
    }

    /// <summary>A promotion over an unseeded copy is retried, not faulted: it answers that nothing is committed and leaves the copy as it was.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionOverUnseededCopyIsRetried(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-promote");
        var cache = new StubCache();
        var gateway = new ScriptedGateway();
        await using var registry = await OpenUnseededAsync(dir, cancellationToken);
        await using var committer = LedGroupsTestKit.CreateElectedCommitter(registry, "n1", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n1", "n1"));

        var first = await committer.PromoteAsync(1UL, cancellationToken);
        var second = await committer.PromoteAsync(1UL, cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((first, second, committer.IsStarted)).IsEqualTo((false, false, false));
        _ = await Assert.That((status.MemberState, status.LastLogIndex)).IsEqualTo((GroupMemberState.Unseeded, 0UL));
        _ = await Assert.That(gateway.Appends.IsEmpty).IsTrue();
    }

    private static async Task<ReplicaGroupRegistry> OpenUnseededAsync(string dir, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, ["n1"], 3, Fingerprint, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }
}
