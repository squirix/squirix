using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A committer that leads its own group without an election leads term one only: a group log an election raised past it is refused before
/// anything is appended, and its verification reports the group blocked. Node n1 owns group n1 over a real group log.
/// </summary>
public sealed class ReplicaCommitterStaticTermTests : ServerUnitTestBase
{
    private const int OlderTermTailEventId = 4010;
    private const int StaticTermAboveOneEventId = 4050;

    /// <summary>A static leader of a fresh log commits in term one, and its followers see term one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticLeaderLeadsTermOne(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-static-term-one");
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogTerm, status.CommitIndex)).IsEqualTo((1UL, 1UL, 1UL));
        _ = await Assert.That(gateway.AppendHeaders.IsEmpty).IsFalse();
        foreach (var (_, header) in gateway.AppendHeaders)
            _ = await Assert.That(header.Term).IsEqualTo(1UL);
    }

    /// <summary>A static leader whose log an election raised to term two refuses every write with no leader authority; nothing is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticLeaderRefusesTermAboveOne(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-static-term-refused");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 2, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, cache);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));
            _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        }

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 2UL, 1UL));
        _ = await Assert.That(committer.IsStarted).IsFalse();
        _ = await Assert.That(gateway.Appends.IsEmpty).IsTrue();
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>
    /// Verification of a static leader whose log moved past term one reports the group blocked on every pass, warns once per log term,
    /// and sends no entry to any follower.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticProbeBlockedAboveTermOne(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-static-term-probe");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 2, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        var log = new EventRecordingLogger();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway, new StubCache(), log);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(log.Count(StaticTermAboveOneEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(StaticTermAboveOneEventId)?.Level).IsEqualTo(LogLevel.Warning);

        await RaiseTermAsync(registry, 3, cancellationToken);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);

        _ = await Assert.That(log.Count(StaticTermAboveOneEventId)).IsEqualTo(2);
        _ = await Assert.That(log.Count(OlderTermTailEventId)).IsEqualTo(0);
        _ = await Assert.That(gateway.Appends.IsEmpty).IsTrue();
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }
}
