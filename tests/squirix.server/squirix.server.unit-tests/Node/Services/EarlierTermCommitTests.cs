using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An entry of an earlier term is committed only together with an entry of the leader's term (#1158): a majority that physically holds the
/// earlier-term entry does not move the durable commit index while the leader's own entry is not on a majority. Node n1 leads its own group
/// n1 in term two over a real group log; its followers are n2 and n3.
/// </summary>
public sealed class EarlierTermCommitTests : ServerUnitTestBase
{
    /// <summary>
    /// The leader and n2 hold the term-one tail entry, a majority; n2 stores no new entry and n3 is down, so the leader-term entry stays
    /// alone. The durable commit index stays at the committed prefix, the tail is not applied, and the promotion is not authorized.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The owned group log is not open.</exception>
    [Test]
    public async Task EarlierTermMajorityLeavesCommitIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-earlier-term-commit");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 2, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Stalled, 2);
        gateway.Set("n3", FollowerMode.Down);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        await using var committer = CreateElectedCommitter(registry, "n1", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n1", "n1"));
        var promoted = await committer.PromoteAsync(2UL, cancellationToken);
        var verification = await committer.VerifyReplicasAsync(cancellationToken);
        var retried = await committer.PromoteAsync(2UL, cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((promoted, retried)).IsEqualTo((false, false));
        _ = await Assert.That(verification).IsEqualTo(ReplicaVerification.Pending);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    /// <summary>
    /// A majority that holds an earlier-term tail entry does not make the applier behind: the entry may not be committed by counting
    /// replicas, so the majority index stays at the committed prefix.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierTermTailIsNotBehind(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-earlier-term-progress");
        await SeedAsync(dir, cancellationToken);
        await SeedTailAsync(dir, 2, cancellationToken, "k1");
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Stalled, 2);
        gateway.Set("n3", FollowerMode.Down);
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var committer = CreateElectedCommitter(registry, "n1", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n1", "n1"));
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(cancellationToken);

        var progress = committer.ApplyProgress();

        _ = await Assert.That(progress.IsBehind).IsFalse();
        _ = await Assert.That(progress.MajorityIndex).IsEqualTo(1UL);
    }
}
