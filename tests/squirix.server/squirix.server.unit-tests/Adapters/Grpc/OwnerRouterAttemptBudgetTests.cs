using System;
using System.Collections.Generic;
using System.Text;
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
using Squirix.Transport.Grpc;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>The waits, reroutes and fallbacks of one call, on the entry node and on the nodes it is forwarded to, share one attempt budget.</summary>
/// <remarks>Chained routers on one fake clock emulate the deadline a forward carries to its target.</remarks>
[Immutable]
public sealed class OwnerRouterAttemptBudgetTests
{
    private const string Entry = "node-a";
    private const string FirstMember = "node-b";
    private const string SecondMember = "node-c";

    private const int EarlyRefusalMilliseconds = 500;
    private const int EntryWaitMilliseconds = 1500;
    private const int ClientDeadlineMilliseconds = 15000;
    private const int AttemptBudgetMilliseconds = 3000;
    private const int ShortDeadlineMilliseconds = 1000;
    private const int LateForwardSpentMilliseconds = 2500;

    private static readonly TimeSpan ClientOperationDeadline = TimeSpan.FromMilliseconds(ClientDeadlineMilliseconds);
    private static readonly TimeSpan LateForwardSpent = TimeSpan.FromMilliseconds(LateForwardSpentMilliseconds);
    private static readonly TimeSpan EntryWait = TimeSpan.FromMilliseconds(EntryWaitMilliseconds);
    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan EarlyRefusal = TimeSpan.FromMilliseconds(EarlyRefusalMilliseconds);
    private static readonly TimeSpan OverriddenLeaderWait = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan OverriddenDeadline = TimeSpan.FromSeconds(60);

    /// <summary>A forward whose target knows no leader is refused within one attempt, and no other member is tried after the time to refuse passed.</summary>
    /// <param name="internalTarget">Whether the target is told the call is a trusted internal owner RPC, as with mutual TLS.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ForwardedCallIsRefusedWithinOneAttempt(bool internalTarget)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var chain = new RouterChain(clock);
        var entry = new FakeLeaderTable(Entry, default) { Served = false };
        var first = new FakeLeaderTable(FirstMember, default) { OnWait = Spend(clock), LedWithoutAuthority = internalTarget, LedTerm = 3 };
        var second = new FakeLeaderTable(SecondMember, default) { OnWait = Spend(clock), LedWithoutAuthority = internalTarget, LedTerm = 3 };
        chain.Add(FirstMember, CreateRouter(FirstMember, first, internalTarget, clock));
        chain.Add(SecondMember, CreateRouter(SecondMember, second, internalTarget, clock));
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(chain.RunAsync(CreateRouter(Entry, entry, false, clock)));

        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That(string.Join(',', chain.Forwards)).IsEqualTo(FirstMember);
        _ = await Assert.That(Describe(internalTarget ? first.AuthorityWaits : first.Waits)).IsEqualTo(Times(LeaderWait));
        _ = await Assert.That(second.Waits.Count + second.AuthorityWaits.Count).IsEqualTo(0);
        _ = await Assert.That(clock.GetUtcNow() - DateTimeOffset.UnixEpoch).IsLessThanOrEqualTo(RpcAttemptTimeout.PerAttempt - ElectionTimerOptions.LeaderRefusalMargin);
    }

    /// <summary>A target the entry node reached late in its own wait waits only for what is left of the attempt budget.</summary>
    [Test]
    public async Task LateLeaderCapsTargetWait()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var chain = new RouterChain(clock);
        var entry = new FakeLeaderTable(Entry, default) { OnWait = Spend(clock, EntryWait), AfterWait = new LeaderRoute(FirstMember, 2) };
        var first = new FakeLeaderTable(FirstMember, default) { OnWait = Spend(clock), LedWithoutAuthority = true, LedTerm = 3 };
        chain.Add(FirstMember, CreateRouter(FirstMember, first, true, clock));
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(chain.RunAsync(CreateRouter(Entry, entry, false, clock)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(Describe(first.AuthorityWaits)).IsEqualTo(Times(LeaderWait - EntryWait));
    }

    /// <summary>The wait after a stale answer takes only what the first wait left of the attempt budget.</summary>
    [Test]
    public async Task StaleRerouteSharesBudget()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default) { OnWait = Spend(clock, EntryWait), AfterWait = new LeaderRoute(FirstMember, 2) };
        var attempts = new OwnerRouterRerouteTests.Attempts(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm)));
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.LeaderChangedDetail);
        _ = await Assert.That(Describe(table.Waits)).IsEqualTo(Times(LeaderWait, LeaderWait - EntryWait));
    }

    /// <summary>A first wait that took the whole time to refuse leaves no reroute: the stale answer ends the call without a second wait.</summary>
    [Test]
    public async Task StaleRerouteAfterFullWaitIsRefused()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default) { OnWait = Spend(clock), AfterWait = new LeaderRoute(FirstMember, 2) };
        var attempts = new OwnerRouterRerouteTests.Attempts(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm)));
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.LeaderChangedDetail);
        _ = await Assert.That(Describe(table.Waits)).IsEqualTo(Times(LeaderWait));
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(FirstMember);
    }

    /// <summary>A refusal that arrives once the time to refuse passed is relayed: no other member is tried.</summary>
    [Test]
    public async Task FallbackSkippedAfterRefusalTime()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default) { Served = false };
        var refusal = ServerOpContract.NoLeaderAuthority();
        var attempts = new OwnerRouterRerouteTests.Attempts(refusal) { OnAttempt = () => clock.Advance(LeaderWait) };
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock)));

        _ = await Assert.That(failure).IsSameReferenceAs(refusal);
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(1);
    }

    /// <summary>A refusal that arrives early leaves the budget for the fallback, which carries what is left as its deadline.</summary>
    [Test]
    public async Task EarlyRefusalFallsBack()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default) { Served = false };
        var attempts = new OwnerRouterRerouteTests.Attempts(ServerOpContract.NoLeaderAuthority()) { OnAttempt = () => clock.Advance(EarlyRefusal) };
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        _ = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock));

        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(2);
        _ = await Assert.That(attempts.Remaining[1]).IsEqualTo(RpcAttemptTimeout.PerAttempt - EarlyRefusal);
    }

    /// <summary>A forward carries the attempt budget as its deadline, never more than the deadline of the caller,.</summary>
    /// <param name="deadlineMilliseconds">The deadline of the caller.</param>
    /// <param name="forwardMilliseconds">The expected deadline of the forward.</param>
    [Test]
    [Arguments(ClientDeadlineMilliseconds, AttemptBudgetMilliseconds)]
    [Arguments(ShortDeadlineMilliseconds, ShortDeadlineMilliseconds)]
    public async Task ForwardCarriesAttemptDeadline(int deadlineMilliseconds, int forwardMilliseconds)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, new LeaderRoute(FirstMember, 2));
        var attempts = new OwnerRouterRerouteTests.Attempts();
        var caller = TimeSpan.FromMilliseconds(deadlineMilliseconds);
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + caller, clock);

        _ = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock));

        _ = await Assert.That(Describe(attempts.Remaining)).IsEqualTo(Times(TimeSpan.FromMilliseconds(forwardMilliseconds)));
    }

    /// <summary>A local attempt after a forward sees the deadline of the caller, not the narrowed one of the forward: the scope ended with the forward.</summary>
    [Test]
    public async Task LocalAfterForwardSeesCallerDeadline()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, new LeaderRoute(FirstMember, 2)) { AfterRefute = new LeaderRoute(Entry, 3) };
        var attempts = new OwnerRouterRerouteTests.Attempts(new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm)));
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        _ = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock));

        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo($"{FirstMember},{OwnerRouterRerouteTests.Attempts.Local}");
        _ = await Assert.That(Describe(attempts.Remaining)).IsEqualTo(Times(RpcAttemptTimeout.PerAttempt, ClientOperationDeadline));
    }

    /// <summary>A call this node serves itself runs under the deadline of the caller.</summary>
    [Test]
    public async Task SelfRouteSeesCallerDeadline()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, new LeaderRoute(Entry, 2));
        var attempts = new OwnerRouterRerouteTests.Attempts();
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        _ = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock));

        _ = await Assert.That(Describe(attempts.Remaining)).IsEqualTo(Times(ClientOperationDeadline));
    }

    /// <summary>A forward that ran out of its deadline late in the attempt relays the deadline failure: it is neither rerouted nor tried on another member.</summary>
    [Test]
    public async Task LateForwardDeadlineFailureIsRelayed()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default) { Served = false };
        var expired = new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline Exceeded"));
        var attempts = new OwnerRouterRerouteTests.Attempts(expired) { OnAttempt = () => clock.Advance(LateForwardSpent) };
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + ClientOperationDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock)));

        _ = await Assert.That(failure).IsSameReferenceAs(expired);
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(1);
        _ = await Assert.That(table.Refuted.Count).IsEqualTo(0);
    }

    /// <summary>A call without a deadline still forwards under the attempt budget.</summary>
    [Test]
    public async Task ForwardWithoutDeadlineCarriesBudget()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, new LeaderRoute(FirstMember, 2));
        var attempts = new OwnerRouterRerouteTests.Attempts();

        _ = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, false, clock));

        _ = await Assert.That(Describe(attempts.Remaining)).IsEqualTo(Times(RpcAttemptTimeout.PerAttempt));
    }

    /// <summary>A leader wait override above the per-attempt timeout extends the attempt budget, so the first wait takes the whole override.</summary>
    [Test]
    public async Task LeaderWaitOverrideExtendsAttemptBudget()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Entry, default);
        var attempts = new OwnerRouterRerouteTests.Attempts();
        using var deadline = ServerRpcDeadlineContext.Push(clock.GetUtcNow().UtcDateTime + OverriddenDeadline, clock);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            attempts.RunAsync(CreateRouter(Entry, table, false, clock, OverriddenLeaderWait)));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(Describe(table.Waits)).IsEqualTo(Times(OverriddenLeaderWait));
    }

    private static string Times(params TimeSpan[] times) => Describe(times);

    private static string Describe<T>(IEnumerable<T> items)
    {
        var text = new StringBuilder();
        foreach (var item in items)
            _ = text.Append(item).Append(';');

        return text.ToString();
    }

    private static Action<TimeSpan> Spend(FakeTimeProvider clock) => clock.Advance;

    private static Action<TimeSpan> Spend(FakeTimeProvider clock, TimeSpan fixedTime) => _ => clock.Advance(fixedTime);

    private static OwnerRouter CreateRouter(string self, IGroupLeaderTable table, bool internalCall, TimeProvider clock) =>
        CreateRouter(self, table, internalCall, clock, LeaderWait);

    private static OwnerRouter CreateRouter(string self, IGroupLeaderTable table, bool internalCall, TimeProvider clock, TimeSpan leaderWait)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(FirstMember);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(internalCall);
        return new OwnerRouter(
            ownership.Instance(),
            invocation.Instance(),
            RingAgreements.Create(),
            table,
            leaderWait,
            clock,
            OwnerRouters.Locator(Entry, FirstMember, SecondMember));
    }

    /// <summary>Routers of several nodes on one fake clock; a forward runs the router of its target under the deadline it carries, as gRPC propagates it.</summary>
    [Mutable]
    private sealed class RouterChain
    {
        private readonly FakeTimeProvider _clock;
        private readonly List<(string Node, OwnerRouter Router)> _routers = [];

        internal RouterChain(FakeTimeProvider clock)
        {
            _clock = clock;
        }

        internal List<string> Forwards { get; } = [];

        internal void Add(string node, OwnerRouter router) => _routers.Add((node, router));

        internal Task<string> RunAsync(OwnerRouter router) => router.ExecuteAsync(
            "cache",
            "key",
            this,
            static (chain, target, cancellationToken) => chain.ForwardAsync(target, cancellationToken),
            static (_, _) => Task.FromResult("served"),
            CancellationToken.None);

        private OwnerRouter RouterOf(string node)
        {
            for (var i = 0; i < _routers.Count; i++)
            {
                if (string.Equals(_routers[i].Node, node, StringComparison.Ordinal))
                    return _routers[i].Router;
            }

            throw new InvalidOperationException($"No router for '{node}'.");
        }

        private async Task<string> ForwardAsync(string target, CancellationToken cancellationToken)
        {
            Forwards.Add(target);
            var carried = _clock.GetUtcNow().UtcDateTime + (ServerRpcDeadlineContext.GetRemainingBudget() ?? TimeSpan.Zero);
            using (ServerRpcDeadlineContext.Push(carried, _clock))
                return await RouterOf(target).ExecuteAsync(
                    "cache",
                    "key",
                    this,
                    static (_, _, _) => Task.FromResult("forwarded again"),
                    static (_, _) => Task.FromResult("served"),
                    cancellationToken).ConfigureAwait(false);
        }
    }
}
