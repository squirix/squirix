using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>
/// An entry node outside a replica group reaches its leader when the ring owner is unreachable: the single reroute goes to the next member, and
/// the leader a member names is learned for the next calls.
/// </summary>
/// <remarks>Five nodes, three replicas: the group of <see cref="Owner" /> is <see cref="Owner" />, <see cref="Next" /> and <see cref="Leader" />, without this node.</remarks>
[Immutable]
public sealed class OwnerRouterFallbackTests
{
    private const string Leader = "node-d";
    private const string Next = "node-c";
    private const string Owner = "node-b";
    private const string Self = "node-a";

    private static readonly byte[] Fingerprint = [1];

    private static readonly string[] Nodes = [Self, Owner, Next, Leader, "node-e"];

    /// <summary>A forward that could not connect to the owner takes the reroute to the next member with the same request; that member is learned.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreachableOwnerFallsBackToMember(CancellationToken cancellationToken)
    {
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var attempts = new Attempts(Unreachable());

        var answer = await attempts.RunAsync(CreateRouter(table), cancellationToken);

        _ = await Assert.That(answer).IsEqualTo(Next);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{Owner},{Next}");
        _ = await Assert.That(attempts.SameRequest).IsTrue();
        _ = await Assert.That((table.TryGetLearnedLeader(Owner, out var learned), learned)).IsEqualTo((true, new LeaderRoute(Next, 0)));
    }

    /// <summary>The leader a member names is learned, so the next call of the group goes to it in one attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LearnedHintRoutesNextCall(CancellationToken cancellationToken)
    {
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var router = CreateRouter(table);
        var first = new Attempts(StaleOwner(Leader, 4));
        _ = await first.RunAsync(router, cancellationToken);

        var next = new Attempts();
        _ = await next.RunAsync(router, cancellationToken);

        _ = await Assert.That(string.Join(',', first.Targets)).IsEqualTo($"{Owner},{Leader}");
        _ = await Assert.That(string.Join(',', next.Targets)).IsEqualTo(Leader);
        _ = await Assert.That((table.TryGetLearnedLeader(Owner, out var learned), learned)).IsEqualTo((true, new LeaderRoute(Leader, 4)));
    }

    /// <summary>
    /// The fallback is the single reroute: when the member it reached names the leader, the leader is learned and the call ends as leader-changed
    /// after two attempts, so the next attempt of the client goes straight to the leader.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FallbackConsumesTheReroute(CancellationToken cancellationToken)
    {
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var attempts = new Attempts(Unreachable(), StaleOwner(Leader, 5));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.LeaderChangedDetail);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{Owner},{Next}");
        _ = await Assert.That(attempts.SameRequest).IsTrue();
        _ = await Assert.That((table.TryGetLearnedLeader(Owner, out var learned), learned)).IsEqualTo((true, new LeaderRoute(Leader, 5)));
    }

    /// <summary>A leader learned from a hint that turns out unreachable after the reroute is forgotten, and no third attempt runs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreachableHintIsForgotten(CancellationToken cancellationToken)
    {
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var unreachable = Unreachable();
        var attempts = new Attempts(StaleOwner(Leader, 5), unreachable);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(unreachable);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{Owner},{Leader}");
        _ = await Assert.That(table.TryGetLearnedLeader(Owner, out _)).IsFalse();
    }

    /// <summary>Without elections nothing is learned, and an unreachable owner ends the call after its one attempt, as before leader routing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlagOffIgnoresLearnedRoutes(CancellationToken cancellationToken)
    {
        var table = new StaticLeaderTable(Self);
        table.Learn(Owner, new LeaderRoute(Leader, 4));
        var unreachable = Unreachable();
        var attempts = new Attempts(unreachable);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(table.TryGetLearnedLeader(Owner, out _)).IsFalse();
        _ = await Assert.That(failure).IsSameReferenceAs(unreachable);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(Owner);
    }

    /// <summary>
    /// Only the failed connect this node's forwarder raises takes the fallback: the same detail relayed from a peer, a transport failure after
    /// the call may have reached the owner, and an unknown commit outcome end the call after one attempt.
    /// </summary>
    /// <param name="kind">The failure of the first attempt.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task TransportMarkerOnlyFromForwarder(int kind, CancellationToken cancellationToken)
    {
        RpcException[] failures =
        [
            new(new Status(StatusCode.Unavailable, ServerOpContract.OwnerUnreachableDetail)),
            new(new Status(StatusCode.Unavailable, ServerOpContract.OwnerUnreachableDetail, new IOException("connection reset"))),
            new(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)),
        ];
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var attempts = new Attempts(failures[kind]);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(failures[kind]);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(Owner);
        _ = await Assert.That(table.TryGetLearnedLeader(Owner, out _)).IsFalse();
    }

    /// <summary>
    /// A forward that timed out, as when the host of the owner does not answer or its connect outlasts the per-attempt timeout, may have reached
    /// the owner: it never falls back, and the client gets the timeout.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutForwardDoesNotFallBack(CancellationToken cancellationToken)
    {
        await using var registry = CreateRegistry();
        var table = CreateTable(registry);
        var timedOut = new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout."));
        var attempts = new Attempts(timedOut);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(timedOut);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(Owner);
    }

    /// <summary>A group this node serves follows its election state alone: an unreachable leader is not rerouted and no hint is learned for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServedGroupDoesNotFallBack(CancellationToken cancellationToken)
    {
        var table = new FakeLeaderTable(Self, new LeaderRoute(Owner, 2));
        var unreachable = Unreachable();
        var attempts = new Attempts(unreachable);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(CreateRouter(table), cancellationToken));

        _ = await Assert.That(failure).IsSameReferenceAs(unreachable);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(Owner);
        _ = await Assert.That(table.Refuted.Count + table.Learned.Count).IsEqualTo(0);
    }

    private static RpcException Unreachable() => OwnerUnreachableFailure.Create(new HttpRequestException(HttpRequestError.ConnectionError, "connection refused"));

    private static RpcException StaleOwner(string leader, ulong term) => StaleOwnerFailure.Create(leader, Owner, term);

    /// <summary>Creates the registry of this node without opening it: the table reads no election state, so no group of the test is served.</summary>
    /// <returns>The registry.</returns>
    private static ReplicaGroupRegistry CreateRegistry() =>
        new(Path.Join(Path.GetTempPath(), "squirix-fallback-unopened"), [Self, Leader, "node-e"], 3, Fingerprint, 1UL, NullLoggerFactory.Instance);

    private static ReplicaLeaderTable CreateTable(ReplicaGroupRegistry registry) => new(registry, Self, Locator());

    private static ReplicaGroupLocator Locator() => new(new PhysicalNodeRing(Nodes), 3);

    private static OwnerRouter CreateRouter(IGroupLeaderTable table)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Owner);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(false);
        return new OwnerRouter(ownership.Instance(), invocation.Instance(), RingAgreements.Create(), table, OwnerRouters.LeaderWait, TimeProvider.System, Locator());
    }

    /// <summary>Records each attempt with the request it carried, and fails the attempts in order with the configured failures; later attempts answer with their target.</summary>
    [Mutable]
    private sealed class Attempts
    {
        private readonly Queue<RpcException> _failures;
        private readonly object _request = new();
        private readonly List<object> _requests = [];

        internal Attempts(params RpcException[] failures)
        {
            _failures = new Queue<RpcException>(failures);
        }

        internal bool SameRequest => _requests.TrueForAll(r => ReferenceEquals(r, _request));

        internal List<string> Targets { get; } = [];

        internal Task<string> RunAsync(OwnerRouter router, CancellationToken cancellationToken) => router.ExecuteAsync(
            "cache",
            "key",
            (Attempts: this, Request: _request),
            static (s, target, _) => s.Attempts.RecordAsync(target, s.Request),
            static (s, _) => s.Attempts.RecordAsync("local", s.Request),
            cancellationToken);

        private Task<string> RecordAsync(string target, object request)
        {
            Targets.Add(target);
            _requests.Add(request);
            return _failures.TryDequeue(out var failure) ? Task.FromException<string>(failure) : Task.FromResult(target);
        }
    }
}
