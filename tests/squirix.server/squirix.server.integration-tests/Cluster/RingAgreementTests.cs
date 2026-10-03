using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster;

/// <summary>Nodes that were started with different peer lists refuse to serve on inconsistent ownership.</summary>
public sealed class RingAgreementTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string RingMismatchCode = "ring-mismatch";
    private const string RingFencedCode = "ring-fenced";

    /// <summary>A call forwarded by a node whose ring differs from the owner is refused before it executes, and both nodes stop serving.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardFromNodeWithOtherRingIsRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartDivergentClusterAsync(cancellationToken);
        var key = FindKeyBothRingsAssign("n2");
        var operationId = RpcOperationIdentity.New();

        using var channel = CreateGrpcChannel(cluster["n1"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.SetEntryAsync(CreateSetRequest(operationId, key), cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refused.Trailers.GetValue("squirix-error-code")).IsEqualTo(RingMismatchCode);

        await AssertNothingExecutedAsync(cluster["n2"], key, cancellationToken);
        await AssertNotReadyAsync(cluster["n1"], cancellationToken);
        await AssertNotReadyAsync(cluster["n2"], cancellationToken);

        var later = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = FindKeyBothRingsAssign("n1") }, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(later.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(later.Trailers.GetValue("squirix-error-code")).IsEqualTo(RingFencedCode);
    }

    /// <summary>The refusal also works in the other direction: a node with the longer peer list cannot forward into the node with the shorter one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardIntoNodeWithOtherRingIsRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartDivergentClusterAsync(cancellationToken);
        var key = FindKeyBothRingsAssign("n1");
        var operationId = RpcOperationIdentity.New();

        using var channel = CreateGrpcChannel(cluster["n2"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.SetEntryAsync(CreateSetRequest(operationId, key), cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refused.Trailers.GetValue("squirix-error-code")).IsEqualTo(RingMismatchCode);

        await AssertNothingExecutedAsync(cluster["n1"], key, cancellationToken);
        await AssertNotReadyAsync(cluster["n1"], cancellationToken);
        await AssertNotReadyAsync(cluster["n2"], cancellationToken);
    }

    /// <summary>A forwarded get-or-add between nodes with different rings is refused and nothing executes on the owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddForwardIsRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartDivergentClusterAsync(cancellationToken);
        var key = FindKeyBothRingsAssign("n2");

        using var channel = CreateGrpcChannel(cluster["n1"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetOrAddAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "must-not-run", Version = 1 }.MapToProto(),
        };
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetOrAddAsync(request, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refused.Trailers.GetValue("squirix-error-code")).IsEqualTo(RingMismatchCode);
        await AssertNothingExecutedAsync(cluster["n2"], key, cancellationToken);
    }

    /// <summary>Nodes with identical peer lists forward as before and stay ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdenticalPeerListsForwardAndStayReady(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("n1", "n2", cancellationToken: cancellationToken);
        var key = FindKeyOwnedByInShortRing("n2");

        using var channel = CreateGrpcChannel(cluster["n1"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var response = await client.SetEntryAsync(CreateSetRequest(RpcOperationIdentity.New(), key), cancellationToken: cancellationToken);

        _ = await Assert.That(response).IsNotNull();
        var stored = await cluster["n2"].GetRequiredService<PhysicalCache<object?>>().GetValueAsync(new CacheKey(CacheName, key), cancellationToken);
        _ = await Assert.That(stored.Found).IsTrue();
        await AssertReadyAsync(cluster["n1"], cancellationToken);
        await AssertReadyAsync(cluster["n2"], cancellationToken);
    }

    private static SetEntryAsyncRequest CreateSetRequest(string operationId, string key) => new()
    {
        OperationId = operationId,
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = "must-not-run", Version = 1 }.MapToProto(),
    };

    private static string FindKeyOwnedByInShortRing(string owner)
    {
        var shortRing = RuntimeServiceRegistration.CreateHashLocator(["n1", "n2"]);
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"ring-agreement-{i}";
            if (string.Equals(shortRing.GetOwner(CacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' was found.");
    }

    private static string FindKeyBothRingsAssign(string owner)
    {
        var shortRing = RuntimeServiceRegistration.CreateHashLocator(["n1", "n2"]);
        var longRing = RuntimeServiceRegistration.CreateHashLocator(["n1", "n2", "n3"]);
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"ring-agreement-{i}";
            if (string.Equals(shortRing.GetOwner(CacheName, candidate), owner, StringComparison.Ordinal) &&
                string.Equals(longRing.GetOwner(CacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' in both rings was found.");
    }

    private static async Task AssertNothingExecutedAsync(ITestNodeHost owner, string key, CancellationToken cancellationToken)
    {
        var stored = await owner.GetRequiredService<PhysicalCache<object?>>().GetValueAsync(new CacheKey(CacheName, key), cancellationToken);
        var idempotency = owner.GetRequiredService<RpcMutationIdempotencyStore>();

        _ = await Assert.That(stored.Found).IsFalse();
        _ = await Assert.That(idempotency.RecordCount).IsEqualTo(0);
        _ = await Assert.That(idempotency.ExecutionCount).IsEqualTo(0);
    }

    private async Task AssertNotReadyAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(node.Uri, "/health/ready"), cancellationToken);

        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    private async Task AssertReadyAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(node.Uri, "/health/ready"), cancellationToken);

        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Starts n1 with the peer list [n1, n2] and n2 with the peer list [n1, n2, n3].</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A cluster owning both running nodes.</returns>
    private async Task<TestCluster<IntegrationStartOptions>> StartDivergentClusterAsync(CancellationToken cancellationToken)
    {
        var n1 = new ClusterNode("n1", GetNextHttpUri());
        var n2 = new ClusterNode("n2", GetNextHttpUri());
        var n3 = new ClusterNode("n3", GetNextHttpUri());
        var cluster = CreateCluster([n1, n2]);
        try
        {
            _ = await cluster.StartNodeAsync(n1, [n1, n2], null, cancellationToken);
            _ = await cluster.StartNodeAsync(n2, [n1, n2, n3], null, cancellationToken);
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
        finally
        {
            ListenPortPool.IntegrationTests.ReleasePort(n3.Uri.Port);
        }
    }
}
