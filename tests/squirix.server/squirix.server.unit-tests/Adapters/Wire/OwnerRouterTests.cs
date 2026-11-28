using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>With the static leader table the router runs a call where it ran before leader routing: locally on the owner, else forwarded once to it.</summary>
[Immutable]
public sealed class OwnerRouterTests
{
    private const string Remote = "node-b";
    private const string Self = "node-a";

    /// <summary>A client call for a key owned by another node is forwarded to that node.</summary>
    [Test]
    public async Task ClientCallForRemoteKeyIsForwarded()
    {
        var calls = new RouterCalls();

        _ = await calls.RunAsync(CreateRouter(Remote, false), "cache", "key");

        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(Remote);
    }

    /// <summary>Node identities compare ordinally, so an owner differing only by case is another node.</summary>
    [Test]
    public async Task OwnerDifferingByCaseIsRemote()
    {
        var calls = new RouterCalls();

        _ = await calls.RunAsync(CreateRouter("NODE-A", false), "cache", "key");

        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo("NODE-A");
    }

    /// <summary>A call for a key this node owns runs locally, whether it comes from a client or from a peer.</summary>
    /// <param name="internalCall">Whether the call is a trusted internal owner RPC.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LocalKeyRunsLocally(bool internalCall)
    {
        var calls = new RouterCalls();

        _ = await calls.RunAsync(CreateRouter(Self, internalCall), "cache", "key");

        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(RouterCalls.Local);
    }

    /// <summary>A trusted internal owner RPC that reaches a node which does not own the key is refused with the stale-owner marker.</summary>
    [Test]
    public async Task InternalCallForRemoteKeyIsStaleOwner()
    {
        var calls = new RouterCalls();
        var router = CreateRouter(Remote, true);

        var failure = NodeExceptionAssert.For<ServerCallException>().Throws((Router: router, Calls: calls), static s => _ = s.Calls.RunAsync(s.Router, "cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(failure.Message).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.Error.Trailers.Count).IsEqualTo(1);
        _ = await Assert.That(calls.Targets.Count).IsEqualTo(0);
    }

    /// <summary>A stale-owner refusal of the owner reaches the caller as it was, after one attempt: the static table never reroutes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticTableRelaysStaleOnce(CancellationToken cancellationToken)
    {
        var calls = new RouterCalls(StaleOwnerFailure.Create("node-c", Remote));

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(calls.RunAsync(CreateRouter(Remote, false), "cache", "key"), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(Remote);
    }

    /// <summary>Only a ring consisting of this peer skips owner resolution; either route records the chosen group.</summary>
    /// <param name="singlePeer">Whether this node is the only configured peer.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task StaticTopologyRoutesToExpectedPeer(bool singlePeer)
    {
        var self = new ServerPeer { NodeId = Self, Uri = new Uri("https://localhost:6001") };
        var remote = new ServerPeer { NodeId = Remote, Uri = new Uri("https://localhost:6002") };
        var topology = new TopologyOptions(singlePeer ? [self] : [self, remote]) { NodeId = Self };
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        if (!singlePeer)
        {
            _ = ownership.SelfNodeId.Returns(Self);
            _ = ownership.GetOwner("cache", "key").Returns(Remote);
        }

        await using var provider = new NodeProvider(new NodeServices().AddSquirixClusterLocator(topology));
        var context = new BinaryRequestAccessor { Current = new BinaryRequestContext { Path = "/cache/Get" } };
        var router = new OwnerRouter(
            ownership.Object,
            CreateInvocationState(false),
            RingAgreements.Create(),
            provider.Require<IGroupLeaderTable>(),
            OwnerRouters.LeaderWait,
            TimeProvider.System,
            OwnerRouters.Locator(Self))
        {
            KeyContext = context,
        };
        var calls = new RouterCalls();

        var result = await calls.RunAsync(router, "cache", "key");

        _ = await Assert.That(result).IsEqualTo(singlePeer ? RouterCalls.Local : Remote);
        _ = await Assert.That(context.FindGroup("cache", "key")).IsEqualTo(singlePeer ? Self : Remote);
        if (!singlePeer)
        {
            ownership.SelfNodeId.WasCalled(Times.Once);
            ownership.GetOwner("cache", "key").WasCalled(Times.Once);
        }
    }

    /// <summary>A node fenced by a ring mismatch refuses with the ring-fenced marker before it resolves any owner.</summary>
    /// <param name="soleNode">Whether this node is the only peer.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FencedRouterRefusesBeforeResolvingOwner(bool soleNode)
    {
        var agreement = RingAgreements.Create();
        agreement.ReportOutboundMismatch(Remote);

        // The resolver has no setups: resolving an owner would fail the test with a different exception.
        var router = new OwnerRouter(
            INodeOwnershipResolver.Mock(MockBehavior.Strict).Object,
            CreateInvocationState(false),
            agreement,
            new StaticLeaderTable(Self, soleNode),
            OwnerRouters.LeaderWait,
            TimeProvider.System,
            OwnerRouters.Locator(Self));
        var calls = new RouterCalls();

        var failure = NodeExceptionAssert.For<ServerCallException>().Throws((Router: router, Calls: calls), static s => _ = s.Calls.RunAsync(s.Router, "cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(RingMismatchFailure.IsRefusal(failure)).IsTrue();
        _ = await Assert.That(RingMismatchFailure.IsMismatch(failure)).IsFalse();
    }

    /// <summary>An invalid cache name or key never consults ownership, so the canonical validation error is raised by the local path.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    [Test]
    [Arguments("", "key")]
    [Arguments("   ", "key")]
    [Arguments("bad name", "key")]
    [Arguments("cache", "")]
    public async Task InvalidNameOrKeyRunsLocally(string cacheName, string key)
    {
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        var calls = new RouterCalls();

        _ = await calls.RunAsync(OwnerRouters.Static(ownership.Object, CreateInvocationState(true), Self), cacheName, key);

        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(RouterCalls.Local);
    }

    /// <summary>The local guard reuses the router's group and reads authority on every operation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RoutedGroupUsesCurrentAuthority(CancellationToken cancellationToken)
    {
        var request = new BinaryRequestAccessor { Current = new BinaryRequestContext { Path = "/cache/Get" } };
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        _ = ownership.GetOwner("c", "k").Returns(Self);
        var router = new OwnerRouter(
            ownership.Object,
            CreateInvocationState(false),
            RingAgreements.Create(),
            new StaticLeaderTable(Self),
            OwnerRouters.LeaderWait,
            TimeProvider.System,
            OwnerRouters.Locator(Self))
        {
            KeyContext = request,
        };

        var view = new StrongBox<GroupLeaderView>(new GroupLeaderView(true, true, false, 1, 1, new LeaderRoute(Self, 1)));
        var leaders = IGroupLeaderTable.Mock(MockBehavior.Strict);
        _ = leaders.Read(Self).Returns(_ => view.Value);
        var inner = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
        _ = inner.GetValueAsync("c", "k", cancellationToken)
                 .ReturnsAsync(ValueTask.FromResult(new NodeCacheValueResult<string>(false, null)));
        var guard = new OwnershipGuardCacheDecorator<string>(
            Self,
            INodeLocator.Mock(MockBehavior.Strict).Object,
            leaders.Object,
            true,
            inner.Object,
            request);

        _ = await router.ExecuteAsync(
            "c",
            "k",
            guard,
            static (_, _, _) => throw new InvalidOperationException("The request must stay local."),
            static (cache, token) => cache.GetValueAsync("c", "k", token),
            cancellationToken);

        view.Value = new GroupLeaderView(true, false, false, 2, 2, new LeaderRoute(Remote, 2));
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            ReadAsync(guard, cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(failure.FindTrailer("squirix-leader-node-id")).IsEqualTo(Remote);
        _ = await Assert.That(failure.FindTrailer("squirix-leader-term")).IsEqualTo("2");
        inner.GetValueAsync("c", "k", cancellationToken).WasCalled(Times.Once);
        leaders.Read(Self).WasCalled(Times.Exactly(2));
        ownership.SelfNodeId.WasCalled(Times.Once);
        ownership.GetOwner("c", "k").WasCalled(Times.Once);

        static async Task ReadAsync(OwnershipGuardCacheDecorator<string> cache, CancellationToken token)
        {
            _ = await cache.GetValueAsync("c", "k", token);
        }
    }

    private static IRemoteInvocationState CreateInvocationState(bool internalCall)
    {
        var expectations = IRemoteInvocationState.Mock(MockBehavior.Strict);
        _ = expectations.IsInternalOwnerInvocation.Returns(internalCall);
        return expectations.Object;
    }

    private static OwnerRouter CreateRouter(string owner, bool internalCall)
    {
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        _ = ownership.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(owner);
        return OwnerRouters.Static(ownership.Object, CreateInvocationState(internalCall), Self);
    }

    /// <summary>Records where the router ran each attempt: the forward target, or <see cref="Local" /> for the local path.</summary>
    [Mutable]
    private sealed class RouterCalls
    {
        internal const string Local = "local";

        private readonly Exception? _forwardFailure;

        internal RouterCalls(Exception? forwardFailure = null)
        {
            _forwardFailure = forwardFailure;
        }

        internal List<string> Targets { get; } = [];

        internal Task<string> RunAsync(OwnerRouter router, string cacheName, string key) => router.ExecuteAsync(
            cacheName,
            key,
            this,
            static (calls, target, _) => calls.RecordAsync(target, calls._forwardFailure),
            static (calls, _) => new ValueTask<string>(calls.RecordAsync(Local, null)),
            CancellationToken.None).AsTask();

        private Task<string> RecordAsync(string target, Exception? failure)
        {
            Targets.Add(target);
            return failure == null ? Task.FromResult(target) : Task.FromException<string>(failure);
        }
    }
}
