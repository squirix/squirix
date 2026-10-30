using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A failed connect waits for a replacement leader within the original budget and consumes only one reroute.</summary>
public sealed class OwnerRouterRecoveryTests : ServerUnitTestBase
{
    private const string First = "node-b";
    private const string Group = "group";
    private const string Other = "other";
    private const string Second = "node-c";
    private const string Self = "node-a";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);

    /// <summary>A failed connect waits for a different route and preserves the mutation request.</summary>
    /// <param name="replacement">The node leading the next term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(Second)]
    [Arguments(Self)]
    [Arguments(First)]
    public async Task FailedConnectWaitsForNewLeader(string replacement, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-leader");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());

        var call = scene.RunAsync(cancellationToken);
        var pending = !call.IsCompleted;
        scene.State.ObserveLeaderContact(First, 3UL);
        var sameLeaderPending = !call.IsCompleted;
        if (string.Equals(replacement, Self, StringComparison.Ordinal))
        {
            scene.State.SetElectionDriven(true);
            _ = scene.State.BecomeLeader(4UL);
            _ = scene.State.GrantAuthority(4UL);
        }
        else
        {
            scene.State.ObserveLeaderContact(replacement, 4UL);
        }

        _ = await Assert.That(pending && sameLeaderPending).IsTrue();
        _ = await Assert.That(await call.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(replacement);
        _ = await Assert.That(string.Join(',', scene.Targets)).IsEqualTo($"{First},{replacement}");
        _ = await Assert.That(scene.Requests.TrueForAll(request => ReferenceEquals(request, scene.Request))).IsTrue();
        _ = await Assert.That(scene.Request.OperationId).IsEqualTo("recovery-operation");
    }

    /// <summary>A leader published before the wait is entered is observed without losing its notification.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NewLeaderBeforeWaitIsObserved(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-race");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());
        scene.OnFailure = () => scene.State.ObserveLeaderContact(Second, 4UL);

        var answer = await scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(answer).IsEqualTo(Second);
        _ = await Assert.That(string.Join(',', scene.Targets)).IsEqualTo($"{First},{Second}");
    }

    /// <summary>A timed-out recovery leaves the old leader usable by a later call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitExpiresWithoutRefutingLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-timeout");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        var unreachable = Unreachable();
        scene.Failures.Enqueue(unreachable);
        var call = scene.RunAsync(cancellationToken);
        var pending = !call.IsCompleted;

        scene.Clock.Advance(LeaderWait);
        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(call.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        scene.State.ObserveLeaderContact(First, 3UL);
        var next = await scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(failure).IsSameReferenceAs(unreachable);
        _ = await Assert.That(next).IsEqualTo(First);
        _ = await Assert.That(string.Join(',', scene.Targets)).IsEqualTo($"{First},{First}");
    }

    /// <summary>Caller cancellation ends a recovery wait without sending another request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsRecoveryWait(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-cancel");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var call = scene.RunAsync(cancel.Token);
        var pending = !call.IsCompleted;

        await cancel.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(call.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(scene.Targets.Count).IsEqualTo(1);
    }

    /// <summary>A second connect failure ends the call after its single recovery reroute.</summary>
    /// <param name="staleFirst">Whether a stale reply consumed the reroute before the failed connect.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RecoveryUsesOnlyOneReroute(bool staleFirst, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-limit");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        var second = Unreachable();
        scene.Failures.Enqueue(staleFirst ? StaleOwnerFailure.Create(Second, First, 4UL) : Unreachable());
        scene.Failures.Enqueue(second);
        scene.OnFailure = () => scene.State.ObserveLeaderContact(Second, 4UL);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(second);
        _ = await Assert.That(string.Join(',', scene.Targets)).IsEqualTo($"{First},{Second}");
    }

    /// <summary>A stale reply cannot add a third send after a recovery reroute.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleReplyAfterRecoveryDoesNotSendAgain(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-stale");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());
        scene.Failures.Enqueue(StaleOwnerFailure.Create(First, Second, 5UL));
        scene.OnFailure = () => scene.State.ObserveLeaderContact(Second, 4UL);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.LeaderChangedDetail);
        _ = await Assert.That(scene.Targets.Count).IsEqualTo(2);
    }

    /// <summary>Peer-relayed failures and unknown commit outcomes never enter the recovery wait.</summary>
    /// <param name="kind">The failure to relay.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task AmbiguousFailureDoesNotWait(int kind, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-ambiguous");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        RpcException[] failures =
        [
            new(new Status(StatusCode.Unavailable, ServerOpContract.OwnerUnreachableDetail)),
            new(new Status(StatusCode.Unavailable, ServerOpContract.OwnerUnreachableDetail, new IOException("connection reset"))),
            new(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)),
            new(new Status(StatusCode.DeadlineExceeded, "forward timed out")),
        ];
        scene.Failures.Enqueue(failures[kind]);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(failures[kind]);
        _ = await Assert.That(scene.Targets.Count).IsEqualTo(1);
    }

    /// <summary>The first forward's elapsed time and the refusal margin reduce the recovery wait.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryWaitSharesOriginalBudget(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-budget");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());
        scene.OnFailure = () => scene.Clock.Advance(TimeSpan.FromMilliseconds(500));
        using var deadline = ServerRpcDeadlineContext.Push(scene.Clock.GetUtcNow().UtcDateTime + TimeSpan.FromSeconds(2), scene.Clock);
        var call = scene.RunAsync(cancellationToken);
        var pending = !call.IsCompleted;
        scene.Clock.Advance(TimeSpan.FromMilliseconds(499));
        var beforeBoundary = !call.IsCompleted;

        scene.Clock.Advance(TimeSpan.FromMilliseconds(1));
        _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(call.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(pending && beforeBoundary).IsTrue();
        _ = await Assert.That(scene.Targets.Count).IsEqualTo(1);
    }

    /// <summary>A failure at the refusal boundary leaves no time for a recovery wait.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredRerouteBudgetDoesNotWait(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-expired");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        var unreachable = Unreachable();
        scene.Failures.Enqueue(unreachable);
        scene.OnFailure = () => scene.Clock.Advance(LeaderWait);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(scene.RunAsync(cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(unreachable);
        _ = await Assert.That(scene.Targets.Count).IsEqualTo(1);
    }

    /// <summary>A parked recovery does not block routing a request of another group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryWaitDoesNotHoldOtherGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("router-recovery-independent");
        await using var scene = await Scene.OpenAsync(dir, cancellationToken);
        scene.Failures.Enqueue(Unreachable());
        var waiting = scene.RunAsync(cancellationToken);

        var healthy = await scene.Router.ExecuteAsync("cache", Other, scene.Request, static (_, target, _) => Task.FromResult(target), static (_, _) => Task.FromResult(Self), cancellationToken);
        var pending = !waiting.IsCompleted;
        scene.State.ObserveLeaderContact(Second, 4UL);

        _ = await Assert.That(healthy).IsEqualTo(Other);
        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(await waiting.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsEqualTo(Second);
    }

    private static RpcException Unreachable() => OwnerUnreachableFailure.Create(new HttpRequestException(HttpRequestError.ConnectionError, "connection refused"));

    [Mutable]
    private sealed class Scene : IAsyncDisposable
    {
        private Scene(ReplicaGroupRegistry registry, FakeTimeProvider clock)
        {
            Clock = clock;
            State = registry.StateFor(Group);
            State.ObserveLeaderContact(First, 3UL);
            registry.StateFor(Other).ObserveLeaderContact(Other, 3UL);
            var ownership = new INodeOwnershipResolverCreateExpectations();
            _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
            _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Callback(static (_, key) => string.Equals(key, Other, StringComparison.Ordinal) ? Other : Group);
            var invocation = new IRemoteInvocationStateCreateExpectations();
            _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(false);
            var locator = OwnerRouters.Locator(Self, First, Second, Group, Other);
            Router = new OwnerRouter(ownership.Instance(), invocation.Instance(), RingAgreements.Create(), new ReplicaLeaderTable(registry, Self, locator), LeaderWait, clock, locator);
        }

        internal FakeTimeProvider Clock { get; }

        internal Queue<RpcException> Failures { get; } = [];

        internal Action? OnFailure { get; set; }

        internal MutationRequest Request { get; } = new("recovery-operation");

        internal List<MutationRequest> Requests { get; } = [];

        internal required ReplicaGroupRegistry Registry { get; init; }

        internal OwnerRouter Router { get; }

        internal ReplicaGroupState State { get; }

        internal List<string> Targets { get; } = [];

        public ValueTask DisposeAsync() => Registry.DisposeAsync();

        internal static async Task<Scene> OpenAsync(TempDirectory dir, CancellationToken cancellationToken)
        {
            var clock = new FakeTimeProvider();
            var registry = new ReplicaGroupRegistry(dir, [Group, Other], 3, Fingerprint, 1UL, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = clock };
            try
            {
                await registry.OpenAsync(cancellationToken);
                return new Scene(registry, clock) { Registry = registry };
            }
            catch
            {
                await registry.DisposeAsync();
                throw;
            }
        }

        internal Task<string> RunAsync(CancellationToken cancellationToken) => Router.ExecuteAsync(
            "cache", "key", (Scene: this, Request), static (s, target, _) => s.Scene.SendAsync(target, s.Request), static (s, _) => s.Scene.SendAsync(Self, s.Request), cancellationToken);

        private Task<string> SendAsync(string target, MutationRequest request)
        {
            Targets.Add(target);
            Requests.Add(request);
            if (!Failures.TryDequeue(out var failure))
                return Task.FromResult(target);

            OnFailure?.Invoke();
            return Task.FromException<string>(failure);
        }
    }

    private sealed record MutationRequest(string OperationId);
}
