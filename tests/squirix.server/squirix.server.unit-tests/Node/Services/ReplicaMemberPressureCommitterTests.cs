using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
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
/// A leader that leads its group statically records the memory pressure flag of every follower reply, and asks for a fresh reply when a
/// refusal finds the flag older than half its lifetime, because it sends no heartbeats of its own.
/// </summary>
[Immutable]
public sealed class ReplicaMemberPressureCommitterTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>The flag of a reply to a live append is recorded and lapses after the election timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticLeaderRecordsFlagOfAppendReply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-pressure-record");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.SetMemoryPressure("n2", true);
        var clock = new FakeTimeProvider();
        await using var committer = CreateCommitterOnBudgetClock(registry, gateway, clock);

        _ = await Assert.That(committer.HasMemberPressure()).IsFalse();
        await committer.CommitSetAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken);
        await committer.WaitUntilAsync(static c => c.HasMemberPressure(), cancellationToken);

        // The commit index heartbeat is answered inline, so its reply is recorded before the flag is left to lapse.
        clock.Advance(ReplicaFollowerSender.CommitIndexDebounce);
        clock.Advance(registry.Election.ElectionTimeout);

        _ = await Assert.That(committer.HasMemberPressure()).IsFalse();
    }

    /// <summary>A refusal that finds an old flag asks the followers for a reply, which clears the flag of a member that recovered.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalRefreshesOldFlagWithHeartbeat(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-pressure-refresh");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.SetMemoryPressure("n2", true);
        var clock = new FakeTimeProvider();
        await using var committer = CreateCommitterOnBudgetClock(registry, gateway, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "k1", Entry("k1"), cancellationToken);
        await gateway.SentAsync("n2").WaitAsync(cancellationToken);
        await gateway.SentAsync("n3").WaitAsync(cancellationToken);
        await committer.WaitUntilAsync(static c => c.HasMemberPressure(), cancellationToken);

        // The commit index heartbeat is answered inline, so it is counted before the refusal asks for a reply.
        clock.Advance(ReplicaFollowerSender.CommitIndexDebounce);
        var sent = gateway.Requests;

        gateway.SetMemoryPressure("n2", false);
        clock.Advance(registry.Election.ElectionTimeout / 2);
        var refused = committer.HasMemberPressure();
        await gateway.WaitUntilAsync(g => g.Requests > sent, cancellationToken);
        await committer.WaitUntilAsync(static c => !c.MemberPressure.IsAnyFresh(), cancellationToken);

        _ = await Assert.That(refused).IsTrue();
        _ = await Assert.That(committer.HasMemberPressure()).IsFalse();
    }

    /// <summary>Every read of the member pressure of a committer returns the same instance.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemberPressureIsOneInstancePerCommitter(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-pressure-instance");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitterOnBudgetClock(registry, new ScriptedGateway(), new FakeTimeProvider());

        _ = await Assert.That(committer.MemberPressure).IsSameReferenceAs(committer.MemberPressure);
    }

    /// <summary>An elected leader records the flag of the replies of the followers it catches up, as a static leader does of live appends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElectedLeaderRecordsFlagOfCatchUpReply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-member-pressure");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = registry.TryGetLog("n2", out var log);
        _ = await log!.ObserveTermAsync(2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind);
        gateway.Set("n3", FollowerMode.Behind);
        gateway.SetMemoryPressure("n2", true);
        gateway.SetMemoryPressure("n3", true);
        var cache = new StubCache();
        await using var committer = CreateElectedCommitter(registry, "n2", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));

        _ = await Assert.That(committer.MemberPressure.IsAnyFresh()).IsFalse();
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        _ = await committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n2", NullLogger.Instance, null), cancellationToken);

        _ = await Assert.That(committer.MemberPressure.IsAnyFresh()).IsTrue();
    }
}
