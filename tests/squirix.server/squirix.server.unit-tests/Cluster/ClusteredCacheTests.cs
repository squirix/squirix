using System;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Verifies that cache routing uses the topology's ordinal node identity semantics.</summary>
public sealed class ClusteredCacheTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "key";
    private const string RemoteCallMessage = "The remote cache path was selected.";
    private const string Self = "node-a";

    /// <summary>A pool disposal racing remote execution surfaces Unavailable instead of ObjectDisposedException.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedPolicyMapsToUnavailable(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-clustered-cache-disposed");
        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter), new ServerRpcTimeoutMetrics(meter));
        var policy = new ServerCallPolicy(instrumentation, peer: "node-b");
        await policy.DisposeAsync();

        var peers = new ServerPeer[] { new() { NodeId = "node-b", Uri = new Uri("https://localhost:6500") } };
        await using var pool = new ServerClientPool(peers, new ServerClientPoolArgs { PolicyFactory = _ => policy }, new ServerClientPoolMetrics(meter));
        var cache = new ClusteredCache<string>(Self, new ILogicalNamespacedCacheCreateExpectations<string>().Instance(), RocksDoubles.CreateOwnerLocator("node-b"), pool);

        var exception = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<string>?>(cache.GetEntryAsync(CacheName, Key, cancellationToken));

        _ = await Assert.That(exception.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    /// <summary>Owners differing only by case are remote because node identifiers are ordinal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetEntryAsyncCasedOwnerUsesRemoteCache(CancellationToken cancellationToken)
    {
        var setEntryCalls = new StrongBox<int>();
        var forNodeCalls = new StrongBox<int>();
        await using var clients = CreateThrowingClientPool(forNodeCalls);
        var cache = CreateCache("NODE-A", CreateRecordingCache(setEntryCalls), clients);

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string> { Value = "value" }, cancellationToken));

        _ = await Assert.That(exception.Message).IsEqualTo(RemoteCallMessage);
        _ = await Assert.That(setEntryCalls.Value).IsEqualTo(0);
        _ = await Assert.That(forNodeCalls.Value).IsEqualTo(1);
    }

    /// <summary>Exact owner identities execute the mutation through the local cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetEntryAsyncExactOwnerUsesLocalCache(CancellationToken cancellationToken)
    {
        var setEntryCalls = new StrongBox<int>();
        var forNodeCalls = new StrongBox<int>();
        await using var clients = CreateThrowingClientPool(forNodeCalls);
        var cache = CreateCache(Self, CreateRecordingCache(setEntryCalls), clients);

        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string> { Value = "value" }, cancellationToken);

        _ = await Assert.That(setEntryCalls.Value).IsEqualTo(1);
        _ = await Assert.That(forNodeCalls.Value).IsEqualTo(0);
    }

    private static ClusteredCache<string> CreateCache(string owner, ILogicalNamespacedCache<string> local, IServerClientPool clients) =>
        new(Self, local, RocksDoubles.CreateOwnerLocator(owner), clients);

    /// <summary>Mocks the local cache: entry writes complete and are counted.</summary>
    /// <param name="setEntryCalls">Counts the entry writes.</param>
    /// <returns>The mocked local cache.</returns>
    private static ILogicalNamespacedCache<string> CreateRecordingCache(StrongBox<int> setEntryCalls)
    {
        var expectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = expectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _, _) =>
                         {
                             setEntryCalls.Value++;
                             return ValueTask.CompletedTask;
                         });
        return expectations.Instance();
    }

    /// <summary>Mocks a client pool whose client lookup fails, so selecting the remote path is observable.</summary>
    /// <param name="forNodeCalls">Counts the client lookups.</param>
    /// <returns>The mocked client pool.</returns>
    private static IServerClientPool CreateThrowingClientPool(StrongBox<int> forNodeCalls)
    {
        var expectations = new IServerClientPoolCreateExpectations();
        _ = expectations.Setups.ForNode(Arg.Any<string>())
                        .Callback(_ =>
                         {
                             forNodeCalls.Value++;
                             throw new InvalidOperationException(RemoteCallMessage);
                         });
        _ = expectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        return expectations.Instance();
    }
}
