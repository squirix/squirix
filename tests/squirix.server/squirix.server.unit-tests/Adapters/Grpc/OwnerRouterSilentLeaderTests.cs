using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A router on a follower stops sending calls to a leader it has not heard from for one election timeout and waits for the next one.</summary>
public sealed class OwnerRouterSilentLeaderTests : ServerUnitTestBase
{
    private const string First = "node-b";
    private const string Group = "n2";
    private const string Other = "node-d";
    private const string Second = "node-c";
    private const string Self = "node-a";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);

    /// <summary>A call entering through a follower whose leader went silent parks and makes no forward attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentLeaderWaitsInsteadOfForwarding(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-router-silent-wait");
        var time = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(Group).ObserveLeaderContact(First, 3UL);
        time.Advance(registry.Election.ElectionTimeout);
        var targets = new List<string>();

        var call = RunAsync(registry, time, targets, false);

        var parked = call.IsCompleted;
        time.Advance(LeaderWait);

        _ = await Assert.That(parked).IsFalse();
        _ = await Assert.That(targets.Count).IsEqualTo(0);
        _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(call);
    }

    /// <summary>A trusted internal call reaching a follower whose leader went silent is refused as having no leader, not as stale-owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentLeaderRefusesInternalCall(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-router-silent-internal");
        var time = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(Group).ObserveLeaderContact(First, 3UL);
        var targets = new List<string>();
        var live = NodeExceptionAssert.For<RpcException>().Throws((registry, time, targets), static s => _ = RunAsync(s.registry, s.time, s.targets, true));
        time.Advance(registry.Election.ElectionTimeout);

        var silent = NodeExceptionAssert.For<RpcException>().Throws((registry, time, targets), static s => _ = RunAsync(s.registry, s.time, s.targets, true));

        _ = await Assert.That(live.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(silent.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(silent.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(targets.Count).IsEqualTo(0);
    }

    /// <summary>A parked call resumes as soon as a new leader is named, and goes to it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitResumesOnNewLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-router-silent-resume");
        var time = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(Group).ObserveLeaderContact(First, 3UL);
        time.Advance(registry.Election.ElectionTimeout);
        var targets = new List<string>();
        var call = RunAsync(registry, time, targets, false);

        registry.StateFor(Group).ObserveLeaderContact(Second, 4UL);

        _ = await Assert.That(await call.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(Second);
        _ = await Assert.That(string.Join(',', targets)).IsEqualTo(Second);
    }

    /// <summary>When no leader is heard within the leader wait, the call is refused as having no leader and nothing is forwarded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitTimesOutAsNoLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-router-silent-timeout");
        var time = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(Group).ObserveLeaderContact(First, 3UL);
        time.Advance(registry.Election.ElectionTimeout);
        var targets = new List<string>();
        var call = RunAsync(registry, time, targets, false);

        time.Advance(LeaderWait);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(call);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(targets.Count).IsEqualTo(0);
    }

    /// <summary>A leader heard from within the election timeout is forwarded to at once, without any wait.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshLeaderIsForwardedAtOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-router-fresh-leader");
        var time = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(Group).ObserveLeaderContact(First, 3UL);
        time.Advance(registry.Election.ElectionTimeout - TimeSpan.FromMilliseconds(1));
        var targets = new List<string>();

        var call = RunAsync(registry, time, targets, false);

        _ = await Assert.That(call.IsCompleted).IsTrue();
        _ = await Assert.That(await call).IsEqualTo(First);
        _ = await Assert.That(string.Join(',', targets)).IsEqualTo(First);
    }

    private static Task<string> RunAsync(ReplicaGroupRegistry registry, TimeProvider time, List<string> targets, bool internalCall)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Group);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(internalCall);
        var locator = OwnerRouters.Locator(Self, First, Second, Other, Group);
        var router = new OwnerRouter(
            ownership.Instance(),
            invocation.Instance(),
            RingAgreements.Create(),
            new ReplicaLeaderTable(registry, Self, locator),
            LeaderWait,
            time,
            locator);

        return router.ExecuteAsync(
            "cache",
            "key",
            targets,
            static (sent, target, _) =>
            {
                sent.Add(target);
                return Task.FromResult(target);
            },
            static (sent, _) =>
            {
                sent.Add("local");
                return Task.FromResult("local");
            },
            CancellationToken.None);
    }

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, [Group], 3, Fingerprint, 1UL, NullLoggerFactory.Instance) { ElectionClock = time };
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
