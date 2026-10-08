using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>The adapter resolves key ownership before any idempotency or pipeline work, for every single-key RPC.</summary>
public sealed class SquirixServiceAdapterRoutingTests : DisposableServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Remote = "node-b";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test-adapter-routing");

    /// <summary>A client call for a key owned by another node is forwarded and never reaches the idempotency coordinator or the cache operations.</summary>
    [Test]
    public async Task ForwardedCallSkipsIdempotencyAndCache()
    {
        var invoker = new CapturingCallInvoker(static method => CreateResponse(method));
        await using var policy = CreatePolicy();
        var adapter = CreateAdapter(invoker, policy, false);

        foreach (var call in CreateCalls())
            await call(adapter, new TestServerCallContext());

        string[] methods = ["GetEntry", "GetExpiration", "GetOrAdd", "GetValue", "Remove", "RemoveExpiration", "SetEntry", "Touch", "TryAddEntry", "Update"];
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(methods.Length);
        for (var i = 0; i < methods.Length; i++)
            _ = await Assert.That(invoker.Methods[i]).IsEqualTo(methods[i]);
    }

    /// <summary>A trusted internal owner RPC that reaches a node which does not own the key is refused before idempotency or the cache, reads included.</summary>
    [Test]
    public async Task InternalCallToNonOwnerIsStaleOwner()
    {
        var invoker = new CapturingCallInvoker(static method => CreateResponse(method));
        await using var policy = CreatePolicy();
        var adapter = CreateAdapter(invoker, policy, true);

        foreach (var call in CreateCalls())
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(call(adapter, new TestServerCallContext()));
            _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
            _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        }

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(0);
    }

    /// <summary>A reroute after a stale answer sends the same request, so the leader sees the operation id of the first attempt.</summary>
    [Test]
    public async Task RerouteKeepsOperationId()
    {
        var calls = 0;
        var invoker = new CapturingCallInvoker(
            static method => CreateResponse(method),
            () => ++calls == 1 ? StaleOwnerWithHint("node-c") : null);
        await using var policy = CreatePolicy();
        var adapter = CreateAdapter(invoker, policy, false, new FakeLeaderTable(Self, new LeaderRoute(Remote, 2)));

        _ = await adapter.SetEntry(new SetEntryAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, new TestServerCallContext());

        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(2);
        _ = await Assert.That((invoker.Requests[0] as SetEntryAsyncRequest)?.OperationId).IsEqualTo(OperationId);
        _ = await Assert.That((invoker.Requests[1] as SetEntryAsyncRequest)?.OperationId).IsEqualTo(OperationId);
    }

    /// <summary>With the static table a stale answer of the owner reaches the client unchanged after one forward, without any wait.</summary>
    [Test]
    public async Task FlagOffForwardsOnceToOwner()
    {
        var invoker = new CapturingCallInvoker(static method => CreateResponse(method), static () => StaleOwnerWithHint("node-c"));
        await using var policy = CreatePolicy();
        var adapter = CreateAdapter(invoker, policy, false);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            adapter.SetEntry(new SetEntryAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, new TestServerCallContext()));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static object CreateResponse(string method) => method switch
    {
        "GetEntry" => new GetEntryAsyncResponse(),
        "GetExpiration" => new GetExpirationAsyncResponse(),
        "GetOrAdd" => new GetOrAddAsyncResponse(),
        "GetValue" => new GetValueAsyncResponse(),
        "Remove" => new RemoveAsyncResponse(),
        "RemoveExpiration" => new RemoveExpirationAsyncResponse(),
        "SetEntry" => new SetAsyncResponse(),
        "Touch" => new TouchAsyncResponse(),
        "TryAddEntry" => new TryAddAsyncResponse(),
        "Update" => new UpdateAsyncResponse(),
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, "Unsupported method."),
    };

    private static Func<SquirixServiceAdapter<object?>, ServerCallContext, Task>[] CreateCalls() =>
    [
        static async (adapter, context) => _ = await adapter.GetEntry(new GetEntryAsyncRequest { CacheName = "c", Key = "k" }, context),
        static async (adapter, context) => _ = await adapter.GetExpiration(new GetExpirationAsyncRequest { CacheName = "c", Key = "k" }, context),
        static async (adapter, context) => _ = await adapter.GetOrAdd(new GetOrAddAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, context),
        static async (adapter, context) => _ = await adapter.GetValue(new GetValueAsyncRequest { CacheName = "c", Key = "k" }, context),
        static async (adapter, context) => _ = await adapter.Remove(new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, context),
        static async (adapter, context) => _ = await adapter.RemoveExpiration(new RemoveExpirationAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, context),
        static async (adapter, context) => _ = await adapter.SetEntry(new SetEntryAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, context),
        static async (adapter, context) => _ = await adapter.Touch(
            new TouchAsyncRequest { CacheName = "c", Key = "k", Expiration = CreateEntry().Expiration, OperationId = OperationId },
            context),
        static async (adapter, context) => _ = await adapter.TryAddEntry(new TryAddEntryAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, context),
        static async (adapter, context) => _ = await adapter.Update(new UpdateAsyncRequest { CacheName = "c", Key = "k", Entry = CreateEntry(), OperationId = OperationId }, context),
    ];

    private static RpcException StaleOwnerWithHint(string leader) => new(
        new Status(StatusCode.FailedPrecondition, $"Key is owned by '{leader}', not current node '{Remote}'."),
        new Metadata { { "squirix-error-code", "stale-owner" }, { StaleRouteSignals.LeaderNodeIdMetadataKey, leader }, { StaleRouteSignals.LeaderTermMetadataKey, "3" } });

    private static CacheEntryWire CreateEntry() => new() { Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)) };

    private static SquirixServiceAdapter<object?> CreateAdapter(CapturingCallInvoker invoker, ServerCallPolicy policy, bool internalCall, IGroupLeaderTable? table = null)
    {
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Remote);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(internalCall);
        var gate = new IBackpressureGateCreateExpectations();
        _ = gate.Setups.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .ReturnValue(ValueTask.FromResult((Decision.Accepted(), Lease.Empty)));
        var clientIds = new IBackpressureClientIdResolverCreateExpectations();
        _ = clientIds.Setups.Resolve().ReturnValue("client");
        var pool = new IServerClientPoolCreateExpectations();
        _ = pool.Setups.ForNode(Arg.Any<string>()).ReturnValue(new SquirixCacheService.SquirixCacheServiceClient(invoker));
        _ = pool.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);

        var router = table == null
            ? OwnerRouters.Static(ownership.Instance(), invocation.Instance(), Self)
            : new OwnerRouter(ownership.Instance(), invocation.Instance(), RingAgreements.Create(), table, OwnerRouters.LeaderWait, TimeProvider.System);

        // The cache operations and the coordinator have no setups: any call to them fails the test.
        return new SquirixServiceAdapter<object?>(
            new IGrpcCacheOperationsCreateExpectations<object?>().Instance(),
            router,
            new OwnerRpcForwarder(pool.Instance(), gate.Instance(), clientIds.Instance(), RingAgreements.Create()),
            new IRpcMutationIdempotencyCoordinatorCreateExpectations().Instance(),
            TimeProvider.System);
    }

    private ServerCallPolicy CreatePolicy() => new(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)));
}
