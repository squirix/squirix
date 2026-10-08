using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>The leader view decides the refusal before any append: a stale term first, then a known other leader, then no leader.</summary>
[Immutable]
public sealed class LeaderRefusalTests : ServerUnitTestBase
{
    private const string Self = "n1";

    /// <summary>A node that leads with authority refuses nothing.</summary>
    [Test]
    public async Task AuthorityIsNotRefused()
    {
        var view = new GroupLeaderView(true, true, false, 3, 3, new LeaderRoute(Self, 3));

        _ = await Assert.That(LeaderRefusal.Classify(in view, Self)).IsEqualTo(LeaderRefusalKind.None);
    }

    /// <summary>A leader that saw a higher term refuses with stale-term, the detail and the error code the routing layers recognize.</summary>
    [Test]
    public async Task HigherTermIsStaleTerm()
    {
        var view = new GroupLeaderView(true, false, true, 3, 4, default);

        var kind = LeaderRefusal.Classify(in view, Self);
        var failure = LeaderRefusal.Create(kind, in view, Self, true);

        _ = await Assert.That(kind).IsEqualTo(LeaderRefusalKind.StaleTerm);
        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-term");
        _ = await Assert.That(failure.Trailers.Count).IsEqualTo(1);
    }

    /// <summary>A known other leader is named in the stale-owner refusal and, when elections lead the group, in the hint trailers.</summary>
    [Test]
    public async Task KnownLeaderIsStaleOwnerWithHint()
    {
        var view = new GroupLeaderView(true, false, false, 7, 7, new LeaderRoute("n3", 7));

        var kind = LeaderRefusal.Classify(in view, Self);
        var hinted = LeaderRefusal.Create(kind, in view, Self, true);
        var plain = LeaderRefusal.Create(kind, in view, Self, false);

        _ = await Assert.That(kind).IsEqualTo(LeaderRefusalKind.StaleOwner);
        _ = await Assert.That((hinted.StatusCode, hinted.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "Key is owned by 'n3', not current node 'n1'."));
        _ = await Assert.That(hinted.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That((hinted.Trailers.GetValue("squirix-leader-node-id"), hinted.Trailers.GetValue("squirix-leader-term"))).IsEqualTo(("n3", "7"));
        _ = await Assert.That((plain.Status.Detail, plain.Trailers.Count)).IsEqualTo((hinted.Status.Detail, 1));
    }

    /// <summary>A follower that knows no leader, or knows only itself, refuses retryably.</summary>
    [Test]
    public async Task NoLeaderIsUnavailable()
    {
        var unknown = new GroupLeaderView(true, false, false, 2, 2, default);
        var onlySelf = new GroupLeaderView(true, false, false, 2, 2, new LeaderRoute(Self, 2));

        var failure = LeaderRefusal.Create(LeaderRefusal.Classify(in unknown, Self), in unknown, Self, true);

        _ = await Assert.That(LeaderRefusal.Classify(in unknown, Self)).IsEqualTo(LeaderRefusalKind.NoLeader);
        _ = await Assert.That(LeaderRefusal.Classify(in onlySelf, Self)).IsEqualTo(LeaderRefusalKind.NoLeader);
        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>A leader whose leader-term entry is not committed yet, with no higher term seen, refuses retryably.</summary>
    [Test]
    public async Task PromotionPendingIsUnavailable()
    {
        var view = new GroupLeaderView(true, false, true, 5, 5, default);

        _ = await Assert.That(LeaderRefusal.Classify(in view, Self)).IsEqualTo(LeaderRefusalKind.NoLeader);
    }

    /// <summary>Authority that a caller found unusable refuses retryably, as no leader.</summary>
    [Test]
    public async Task UnusableAuthorityIsUnavailable()
    {
        var view = new GroupLeaderView(true, true, false, 5, 5, new LeaderRoute(Self, 5));

        var failure = LeaderRefusal.Create(LeaderRefusalKind.None, in view, Self, true);

        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }
}
