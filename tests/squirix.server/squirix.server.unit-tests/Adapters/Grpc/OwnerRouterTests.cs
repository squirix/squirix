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

/// <summary>The router tells, before any idempotency or pipeline work, where an inbound single-key RPC runs.</summary>
[Immutable]
public sealed class OwnerRouterTests
{
    private const string Remote = "node-b";
    private const string Self = "node-a";

    /// <summary>A client call for a key owned by another node is forwarded to that node.</summary>
    [Test]
    public async Task ClientCallForRemoteKeyIsForwarded()
    {
        var router = CreateRouter(Remote, false);

        _ = await Assert.That(router.FindRemoteOwner("cache", "key")).IsEqualTo(Remote);
    }

    /// <summary>Node identities compare ordinally, so an owner differing only by case is another node.</summary>
    [Test]
    public async Task OwnerDifferingByCaseIsRemote()
    {
        var router = CreateRouter("NODE-A", false);

        _ = await Assert.That(router.FindRemoteOwner("cache", "key")).IsEqualTo("NODE-A");
    }

    /// <summary>A call for a key this node owns runs locally, whether it comes from a client or from a peer.</summary>
    /// <param name="internalCall">Whether the call is a trusted internal owner RPC.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LocalKeyRunsLocally(bool internalCall)
    {
        var router = CreateRouter(Self, internalCall);

        _ = await Assert.That(router.FindRemoteOwner("cache", "key")).IsNull();
    }

    /// <summary>A trusted internal owner RPC that reaches a node which does not own the key is refused with the stale-owner marker.</summary>
    [Test]
    public async Task InternalCallForRemoteKeyIsStaleOwner()
    {
        var router = CreateRouter(Remote, true);

        var failure = NodeExceptionAssert.For<RpcException>().Throws(router, static r => _ = r.FindRemoteOwner("cache", "key"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
    }

    /// <summary>A node fenced by a ring mismatch refuses with the ring-fenced marker before it resolves any owner.</summary>
    [Test]
    public async Task FencedRouterRefusesBeforeResolvingOwner()
    {
        var agreement = RingAgreements.Create();
        agreement.ReportOutboundMismatch(Remote);

        // The resolver has no setups: resolving an owner would fail the test with a different exception.
        var router = new OwnerRouter(new INodeOwnershipResolverCreateExpectations().Instance(), CreateInvocationState(false), agreement);

        var failure = NodeExceptionAssert.For<RpcException>().Throws(router, static r => _ = r.FindRemoteOwner("cache", "key"));

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
        var router = new OwnerRouter(ownership.Instance(), CreateInvocationState(true), RingAgreements.Create());

        _ = await Assert.That(router.FindRemoteOwner(cacheName, key)).IsNull();
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
        return new OwnerRouter(ownership.Instance(), CreateInvocationState(internalCall), RingAgreements.Create());
    }
}
