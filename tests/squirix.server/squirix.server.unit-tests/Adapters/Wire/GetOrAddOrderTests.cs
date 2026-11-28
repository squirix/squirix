using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>GetOrAdd returns the decision of the atomic cache operation.</summary>
public sealed class GetOrAddOrderTests : DisposableServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test-get-or-add-order");

    /// <summary>An insertion reports that it added the requested value.</summary>
    [Test]
    public async Task AbsentKeyReturnsAddedResult()
    {
        var api = ICacheApi<StoredValue>.Mock(MockBehavior.Strict);
        _ = api.AddOrGetAsync(string.Empty, "k", Arg.Any<NodeCacheEntry<StoredValue>>(), Arg.Any<CancellationToken>(), Arg.Any<ICacheRequestContext?>())
               .ReturnsAsync(ValueTask.FromResult(new NodeCacheAddOrGetResult<StoredValue>(true, default)));

        var response = await CreateAdapter(api.Object).Mutations.GetOrAddAsync(CreateRequest(), CancellationToken.None);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(response.Found).IsTrue();
    }

    /// <summary>A present key that the add leaves alone is read and returned as found, not added.</summary>
    [Test]
    public async Task PresentKeyReturnsItsValue()
    {
        var api = ICacheApi<StoredValue>.Mock(MockBehavior.Strict);
        _ = api.AddOrGetAsync(string.Empty, "k", Arg.Any<NodeCacheEntry<StoredValue>>(), Arg.Any<CancellationToken>(), Arg.Any<ICacheRequestContext?>())
               .ReturnsAsync(ValueTask.FromResult(new NodeCacheAddOrGetResult<StoredValue>(false, new NodeCacheValueResult<StoredValue>(true, StoredValue.FromText("existing")))));

        var response = await CreateAdapter(api.Object).Mutations.GetOrAddAsync(CreateRequest(), CancellationToken.None);

        _ = await Assert.That(response.Added).IsFalse();
        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(response.Value!.StringValue).IsEqualTo("existing");
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static GetOrAddAsyncRequest CreateRequest() => new()
    {
        CacheName = "c",
        Key = "k",
        OperationId = OperationId,
        Entry = new CacheEntryWire { Value = JsonValueKit.Wire("{\"__v\":\"added\"}"), Expiration = TimeSpan.FromMinutes(1) },
    };

    private static RoutedCacheOperations<StoredValue> CreateAdapter(ICacheApi<StoredValue> api)
    {
        var operations = IInboundEndpointCacheOperations<StoredValue>.Mock(MockBehavior.Strict);
        _ = operations.ForCache("c").Returns(api);
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        _ = ownership.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(Self);
        var invocation = IRemoteInvocationState.Mock(MockBehavior.Strict);
        _ = invocation.IsInternalOwnerInvocation.Returns(false);

        // Nothing is forwarded: the pool and the backpressure gate have no setups.
        return new RoutedCacheOperations<StoredValue>(
            operations.Object,
            OwnerRouters.Static(ownership.Object, invocation.Object, Self),
            new OwnerRpcForwarder(
                IServerClientPool.Mock(MockBehavior.Strict).Object,
                IBackpressureGate.Mock(MockBehavior.Strict).Object,
                IBackpressureClientIdResolver.Mock(MockBehavior.Strict).Object,
                RingAgreements.Create()),
            false,
            TimeProvider.System);
    }
}
