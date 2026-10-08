using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

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

        var failure = NodeExceptionAssert.For<RpcException>().Throws((Router: router, Calls: calls), static s => _ = s.Calls.RunAsync(s.Router, "cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.Trailers.Count).IsEqualTo(1);
        _ = await Assert.That(calls.Targets.Count).IsEqualTo(0);
    }

    /// <summary>A stale-owner refusal of the owner reaches the caller as it was, after one attempt: the static table never reroutes.</summary>
    [Test]
    public async Task StaticTableRelaysStaleOnce()
    {
        var calls = new RouterCalls(StaleOwnerFailure.Create("node-c", Remote));

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(calls.RunAsync(CreateRouter(Remote, false), "cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(Remote);
    }

    /// <summary>A node fenced by a ring mismatch refuses with the ring-fenced marker before it resolves any owner.</summary>
    [Test]
    public async Task FencedRouterRefusesBeforeResolvingOwner()
    {
        var agreement = RingAgreements.Create();
        agreement.ReportOutboundMismatch(Remote);

        // The resolver has no setups: resolving an owner would fail the test with a different exception.
        var router = new OwnerRouter(
            new INodeOwnershipResolverCreateExpectations().Instance(),
            CreateInvocationState(false),
            agreement,
            new StaticLeaderTable(Self),
            OwnerRouters.LeaderWait,
            TimeProvider.System);
        var calls = new RouterCalls();

        var failure = NodeExceptionAssert.For<RpcException>().Throws((Router: router, Calls: calls), static s => _ = s.Calls.RunAsync(s.Router, "cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
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
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        var calls = new RouterCalls();

        _ = await calls.RunAsync(OwnerRouters.Static(ownership.Instance(), CreateInvocationState(true), Self), cacheName, key);

        _ = await Assert.That(string.Join(',', calls.Targets)).IsEqualTo(RouterCalls.Local);
    }

    private static IRemoteInvocationState CreateInvocationState(bool internalCall)
    {
        var expectations = new IRemoteInvocationStateCreateExpectations();
        _ = expectations.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(internalCall);
        return expectations.Instance();
    }

    private static OwnerRouter CreateRouter(string owner, bool internalCall)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(owner);
        return OwnerRouters.Static(ownership.Instance(), CreateInvocationState(internalCall), Self);
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
            static (calls, _) => calls.RecordAsync(Local, null),
            CancellationToken.None);

        private Task<string> RecordAsync(string target, Exception? failure)
        {
            Targets.Add(target);
            return failure == null ? Task.FromResult(target) : Task.FromException<string>(failure);
        }
    }
}
