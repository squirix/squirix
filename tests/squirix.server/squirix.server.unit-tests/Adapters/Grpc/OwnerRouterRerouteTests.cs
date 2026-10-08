using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Mappers;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>With an election-led table the router sends a call to the group leader and reroutes it at most once when the route answers as stale.</summary>
[Immutable]
public sealed class OwnerRouterRerouteTests
{
    private const string First = "node-b";
    private const string Group = "node-g";
    private const string Second = "node-c";
    private const string Self = "node-a";

    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);

    /// <summary>A stale-owner refusal that names the leader refutes the route and reroutes once to the named leader.</summary>
    [Test]
    public async Task StaleOwnerReroutesOnceToHint()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var attempts = new Attempts(StaleOwner(Second, 3));

        var answer = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(answer).IsEqualTo(Second);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{First},{Second}");
        _ = await Assert.That(table.Refuted.Count == 1 && table.Refuted[0] == new LeaderRoute(First, 2)).IsTrue();
    }

    /// <summary>A stale-term refusal without a hint waits for the next leader of the table and reroutes to it.</summary>
    [Test]
    public async Task StaleTermReroutesToTableLeader()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2)) { AfterWait = new LeaderRoute(Second, 3) };
        var attempts = new Attempts(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm)));

        _ = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{First},{Second}");
        _ = await Assert.That(table.Waits.Count == 1 && table.Waits[0] == LeaderWait).IsTrue();
    }

    /// <summary>The stale-term error code alone marks a stale route, so the trailer is enough to reroute.</summary>
    [Test]
    public async Task StaleTermTrailerReroutes()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var trailers = new Metadata { { GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, RefusalCodes.StaleTerm }, { StaleRouteSignals.LeaderNodeIdMetadataKey, Second } };
        var attempts = new Attempts(new RpcException(new Status(StatusCode.FailedPrecondition, "deposed"), trailers));

        _ = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{First},{Second}");
    }

    /// <summary>A second stale answer ends the operation as unavailable with the leader-changed detail, after exactly two attempts.</summary>
    [Test]
    public async Task SecondStaleIsUnavailable()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var attempts = new Attempts(StaleOwner(Second, 3), StaleOwner("node-d", 4));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table, false, TimeProvider.System)));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo(StaleRouteSignals.LeaderChangedDetail);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{First},{Second}");
        _ = await Assert.That(table.Refuted.Count).IsEqualTo(2);
    }

    /// <summary>Failures that do not prove a stale route reach the caller unchanged after one attempt.</summary>
    /// <param name="kind">The failure of the first attempt.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task OtherFailuresAreNotRerouted(int kind)
    {
        RpcException[] failures =
        [
            new(new Status(StatusCode.Unavailable, $"Key owner '{First}' is unreachable.")),
            new(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)),
            ServerOpContract.NoLeaderAuthority(),
        ];
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var attempts = new Attempts(failures[kind]);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table, false, TimeProvider.System)));

        _ = await Assert.That(failure).IsSameReferenceAs(failures[kind]);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(First);
        _ = await Assert.That(table.Refuted.Count).IsEqualTo(0);
    }

    /// <summary>A local attempt refused as stale is forwarded once to the leader the table learned.</summary>
    [Test]
    public async Task LocalStaleForwardsOnce()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(Self, 2)) { AfterRefute = new LeaderRoute(First, 3) };
        var attempts = new Attempts(StaleOwner(null, 0));

        _ = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{Attempts.Local},{First}");
    }

    /// <summary>When this node becomes the leader between the attempts, the reroute runs locally.</summary>
    [Test]
    public async Task RerouteToSelfRunsLocally()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2)) { AfterRefute = new LeaderRoute(Self, 3) };
        var attempts = new Attempts(StaleOwner(null, 0));

        _ = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{First},{Attempts.Local}");
    }

    /// <summary>A stale answer with no other route known ends as unavailable instead of repeating the refused attempt.</summary>
    [Test]
    public async Task NoOtherRouteIsUnavailable()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(Self, 2)) { AfterRefute = new LeaderRoute(Self, 2) };
        var attempts = new Attempts(StaleOwner(null, 0));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table, false, TimeProvider.System)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(StaleRouteSignals.LeaderChangedDetail);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(Attempts.Local);
    }

    /// <summary>A reroute never starts once the deadline of the operation passed.</summary>
    [Test]
    public async Task ExpiredBudgetMakesNoAttempt()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var attempts = new Attempts(StaleOwner(Second, 3)) { OnAttempt = () => clock.Advance(TimeSpan.FromSeconds(2)) };
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime.AddSeconds(1), clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table, false, clock)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(StaleRouteSignals.LeaderChangedDetail);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(First);
    }

    /// <summary>The wait for a leader is capped by the remaining deadline, and a wait that finds none refuses without any attempt.</summary>
    [Test]
    public async Task WaitBoundedByRemainingDeadline()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Self, default);
        var attempts = new Attempts();
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime.AddMilliseconds(500), clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table, false, clock)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(table.Waits.Count == 1 && table.Waits[0] == TimeSpan.FromMilliseconds(500)).IsTrue();
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(0);
    }

    /// <summary>A trusted internal call is the single hop: a node that does not lead refuses it naming the leader, and never forwards.</summary>
    [Test]
    public async Task InternalCallNeverForwards()
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(First, 2));
        var attempts = new Attempts();
        var router = CreateRouter(table, true, TimeProvider.System);

        var failure = NodeExceptionAssert.For<RpcException>().Throws((Router: router, Attempts: attempts), static s => _ = s.Attempts.RunAsync(s.Router));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo($"Key is owned by '{First}', not current node '{Self}'.");
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(0);
    }

    /// <summary>A group this node does not serve goes to its ring owner, which names the leader when it does not lead.</summary>
    [Test]
    public async Task UnservedGroupUsesStaticOwner()
    {
        var table = new FakeLeaderTable(Self, default) { Served = false };
        var attempts = new Attempts(StaleOwner(Second, 3));

        _ = await attempts.RunAsync(CreateRouter(table, false, TimeProvider.System));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{Group},{Second}");
        _ = await Assert.That(table.Waits.Count).IsEqualTo(0);
    }

    /// <summary>The leader hint trailers are read as optional: a missing or unreadable term still names the leader.</summary>
    [Test]
    public async Task HintWithoutTermIsRead()
    {
        var trailers = new Metadata
        {
            { GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, "stale-owner" },
            { StaleRouteSignals.LeaderNodeIdMetadataKey, Second },
            { StaleRouteSignals.LeaderTermMetadataKey, "x" },
        };

        _ = await Assert.That(StaleRouteSignals.TryReadStale(new RpcException(new Status(StatusCode.FailedPrecondition, "stale"), trailers), out var hint)).IsTrue();
        _ = await Assert.That(hint).IsEqualTo(new LeaderRoute(Second, 0));
        _ = await Assert.That(StaleRouteSignals.TryReadStale(new RpcException(new Status(StatusCode.Unavailable, RefusalCodes.StaleTerm), trailers), out _)).IsFalse();
    }

    private static RpcException StaleOwner(string? leader, ulong term)
    {
        var failure = StaleOwnerFailure.Create(Group, First);
        if (leader == null)
            return failure;

        var trailers = new Metadata
        {
            { GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, "stale-owner" },
            { StaleRouteSignals.LeaderNodeIdMetadataKey, leader },
            { StaleRouteSignals.LeaderTermMetadataKey, term.ToString(CultureInfo.InvariantCulture) },
        };
        return new RpcException(failure.Status, trailers);
    }

    private static OwnerRouter CreateRouter(IGroupLeaderTable table, bool internalCall, TimeProvider clock)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Group);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(internalCall);
        return new OwnerRouter(ownership.Instance(), invocation.Instance(), RingAgreements.Create(), table, LeaderWait, clock);
    }

    /// <summary>Records each attempt and fails the attempts in order with the configured failures; later attempts answer with their target.</summary>
    [Mutable]
    private sealed class Attempts
    {
        internal const string Local = "local";

        private readonly Queue<RpcException> _failures;

        internal Attempts(params RpcException[] failures)
        {
            _failures = new Queue<RpcException>(failures);
        }

        internal Action? OnAttempt { get; init; }

        internal List<string> Targets { get; } = [];

        internal Task<string> RunAsync(OwnerRouter router) => router.ExecuteAsync(
            "cache",
            "key",
            this,
            static (attempts, target, _) => attempts.RecordAsync(target),
            static (attempts, _) => attempts.RecordAsync(Local),
            CancellationToken.None);

        private Task<string> RecordAsync(string target)
        {
            Targets.Add(target);
            OnAttempt?.Invoke();
            return _failures.TryDequeue(out var failure) ? Task.FromException<string>(failure) : Task.FromResult(target);
        }
    }
}
