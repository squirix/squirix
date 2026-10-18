using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader whose committed entries cannot be applied reports how far it is behind, retries the apply when it is not asked to write, and
/// logs the same fault once. Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class LeaderApplyProgressTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>
    /// A committed entry whose apply failed is reported as behind the majority without the commit gate, and once the apply recovers and a
    /// later write is applied, the applied index equals the majority index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyProgressReportsCommittedUnapplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-progress");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, applier);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        var caughtUp = committer.ApplyProgress();

        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var blocked = committer.ApplyProgress();

        cache.OnApplied = null;
        await committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken);
        var recovered = committer.ApplyProgress();

        _ = await Assert.That(caughtUp).IsEqualTo(new ReplicaApplyProgress(1UL, 1UL));
        _ = await Assert.That((blocked, blocked.IsBehind)).IsEqualTo((new ReplicaApplyProgress(1UL, 2UL), true));
        _ = await Assert.That((recovered, recovered.IsBehind)).IsEqualTo((new ReplicaApplyProgress(3UL, 3UL), false));
    }

    private static void GrantAuthority(ReplicaGroupRegistry registry, ulong term)
    {
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        _ = state.GrantAuthority(term);
    }

    private static async Task TermAsync(ReplicaGroupRegistry registry, ulong term, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(term, cancellationToken);
    }
}
