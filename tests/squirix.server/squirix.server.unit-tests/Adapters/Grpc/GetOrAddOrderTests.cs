using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>GetOrAdd adds with its operation id before it reads, so a retry replays the recorded outcome of its add.</summary>
public sealed class GetOrAddOrderTests : DisposableServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test-get-or-add-order");

    /// <summary>
    /// A retry whose RPC record is gone, of a GetOrAdd whose first attempt added the value, answers that it added it: the add replays the
    /// recorded outcome and nothing reads the value first.
    /// </summary>
    [Test]
    public async Task RetryReplaysTheRecordedAdd()
    {
        var api = new ICacheApiCreateExpectations<object?>();
        _ = api.Setups.TryAddEntryAsync(OperationId, "k", Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(true));

        var response = await CreateAdapter(api.Instance()).GetOrAdd(CreateRequest(), new TestServerCallContext());

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(response.Found).IsTrue();
    }

    /// <summary>A present key that the add leaves alone is read and returned as found, not added.</summary>
    [Test]
    public async Task PresentKeyReturnsItsValue()
    {
        var api = new ICacheApiCreateExpectations<object?>();
        _ = api.Setups.TryAddEntryAsync(OperationId, "k", Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(false));
        _ = api.Setups.GetValueAsync("k", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(new NodeCacheValueResult<object?>(true, "existing")));

        var response = await CreateAdapter(api.Instance()).GetOrAdd(CreateRequest(), new TestServerCallContext());

        _ = await Assert.That(response.Added).IsFalse();
        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(response.Value.StringValue).IsEqualTo("existing");
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static GetOrAddAsyncRequest CreateRequest() => new()
    {
        CacheName = "c",
        Key = "k",
        OperationId = OperationId,
        Entry = new CacheEntryWire { Value = Struct.Parser.ParseJson("{\"__v\":\"added\"}"), Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)) },
    };

    private SquirixServiceAdapter<object?> CreateAdapter(ICacheApi<object?> api)
    {
        var operations = new IGrpcCacheOperationsCreateExpectations<object?>();
        _ = operations.Setups.ForCache("c").ReturnValue(api);
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Self);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(false);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), Self, new IdempotencyMetrics(_testMeter));

        // Nothing is forwarded: the pool and the backpressure gate have no setups.
        return new SquirixServiceAdapter<object?>(
            operations.Instance(),
            new OwnerRouter(ownership.Instance(), invocation.Instance()),
            new OwnerRpcForwarder(
                new IServerClientPoolCreateExpectations().Instance(),
                new IBackpressureGateCreateExpectations().Instance(),
                new IBackpressureClientIdResolverCreateExpectations().Instance()),
            new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance),
            TimeProvider.System);
    }
}
