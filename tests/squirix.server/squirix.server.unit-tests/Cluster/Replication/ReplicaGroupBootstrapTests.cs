using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The first start of a group the node owns: when the owner offers its members a place and completes its own copy, and when it waits.</summary>
[Immutable]
public sealed class ReplicaGroupBootstrapTests : ServerUnitTestBase
{
    private const string Group = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];
    private static readonly string[] Members = ["n1", "n2", "n3"];

    /// <summary>Every member reports an unseeded copy: every one is offered, and then the own copy is complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnseededMembersAreOffered(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-owner");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        var bootstrap = Create(log, gateway);

        var outcome = await bootstrap.RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.Completed, string.Empty));
        _ = await Assert.That(outcome.IsFinal).IsTrue();
        _ = await Assert.That(gateway.Calls).IsEqualTo("status:n2,status:n3,offer:n2,offer:n3");
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    /// <summary>A member that does not answer blocks every offer, and the own copy stays unseeded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreachableMemberBlocksOffers(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-unreachable");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Down("n3");

        var outcome = await Create(log, gateway).RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, "n3"));
        _ = await Assert.That(outcome.IsFinal).IsFalse();
        _ = await Assert.That(gateway.Offers).IsEqualTo(0);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>A member that holds a complete copy blocks every offer, and the outcome names it, even when another member does not answer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteMemberBlocksOffersAndIsNamed(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-member-complete");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Down("n2");
        gateway.Report("n3", Unseeded() with { MemberState = GroupMemberState.Complete });

        var outcome = await Create(log, gateway).RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.MemberHoldsCompleteCopy, "n3"));
        _ = await Assert.That(gateway.Offers).IsEqualTo(0);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>A member that is incomplete, not ready, refusing, of another topology or generation, or of an unknown state blocks every offer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemberUnfitToSeedBlocksOffers(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-unfit");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        var bootstrap = Create(log, gateway);
        gateway.Report("n3", Unseeded());
        ReplicaMemberStatus[] unfit =
        [
            Unseeded() with { MemberState = GroupMemberState.Incomplete },
            Unseeded() with { Ready = false },
            Unseeded() with { RefusalCode = RefusalCodes.NotReady },
            Unseeded() with { TopologyFingerprint = new byte[] { 1 } },
            Unseeded() with { ConfigurationGeneration = 2UL },
            Unseeded() with { MemberState = null },
        ];

        foreach (var status in unfit)
        {
            gateway.Report("n2", in status);

            var outcome = await bootstrap.RunOnceAsync(cancellationToken);

            _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, "n2"));
        }

        _ = await Assert.That(gateway.Offers).IsEqualTo(0);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>A member that refuses the offer keeps the own copy unseeded, although the offers before it were accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedOfferKeepsOwnerUnseeded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-refused");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        gateway.RefuseOffers("n3");

        var outcome = await Create(log, gateway).RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.OfferRefused, "n3", GroupMemberState.Unseeded, FollowerLogRefusal.NotReady));
        _ = await Assert.That(gateway.Calls).IsEqualTo("status:n2,status:n3,offer:n2,offer:n3");
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>A member that is reachable for the status but not for the offer is reported as having given no answer, with no state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferWithoutAnswerIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-no-answer");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        gateway.FailOffer("n3", new IOException("connection lost"), static () => { });

        var outcome = await Create(log, gateway).RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.OfferRefused, "n3", null, "no-answer"));
    }

    /// <summary>A status call that the stopping token canceled and that surfaces as an RPC fault ends the iteration with a cancellation, not as a member to wait for.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledStatusCallPropagates(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-cancel-status");
        await using var log = await OpenAsync(dir, cancellationToken);
        using var stopping = new CancellationTokenSource();
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.FailStatus("n3", new RpcException(new Status(StatusCode.Cancelled, "canceled")), stopping.Cancel);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(Create(log, gateway).RunOnceAsync(stopping.Token));

        _ = await Assert.That(gateway.Offers).IsEqualTo(0);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>An offer call that the stopping token canceled and that surfaces as an RPC fault ends the iteration with a cancellation, not as a refused offer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledOfferCallPropagates(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-cancel-offer");
        await using var log = await OpenAsync(dir, cancellationToken);
        using var stopping = new CancellationTokenSource();
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        gateway.FailOffer("n3", new RpcException(new Status(StatusCode.Cancelled, "canceled")), stopping.Cancel);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(Create(log, gateway).RunOnceAsync(stopping.Token));

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>An iteration that stopped after some offers leaves harmless offers, and the next iteration offers again and completes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondIterationCompletes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-second");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        gateway.RefuseOffers("n3");
        var bootstrap = Create(log, gateway);
        var first = await bootstrap.RunOnceAsync(cancellationToken);
        gateway.Report("n3", Unseeded());

        var second = await bootstrap.RunOnceAsync(cancellationToken);

        _ = await Assert.That(first.Result).IsEqualTo(ReplicaBootstrapResult.OfferRefused);
        _ = await Assert.That(second.Result).IsEqualTo(ReplicaBootstrapResult.Completed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
    }

    /// <summary>A copy that is complete already ends the start at once and calls nobody.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompleteOwnerCallsNobody(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-complete");
        await using var log = await OpenAsync(dir, cancellationToken);
        await log.CompleteBootstrapAsync(cancellationToken);
        var gateway = new ScriptedBootstrapGateway();

        var outcome = await Create(log, gateway).RunOnceAsync(cancellationToken);

        _ = await Assert.That(outcome).IsEqualTo(new ReplicaBootstrapOutcome(ReplicaBootstrapResult.AlreadyComplete, string.Empty));
        _ = await Assert.That(outcome.IsFinal).IsTrue();
        _ = await Assert.That(gateway.Calls).IsEqualTo(string.Empty);
    }

    private static ReplicaGroupBootstrap Create(FollowerLog log, ScriptedBootstrapGateway gateway) =>
        new(log, gateway, Members, new ReplicaRpcHeader(Group, Fingerprint, 1UL, 0UL, string.Empty, Group), TimeSpan.FromHours(1));

    private static ReplicaMemberStatus Unseeded() => new(GroupMemberState.Unseeded, true, Fingerprint, 1UL, string.Empty);

    private static async Task<FollowerLog> OpenAsync(string dir, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, Group, GroupComposition.Create(Group), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenAsync(cancellationToken);
        return log;
    }
}
