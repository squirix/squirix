using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.SmokeTests.Logging;

/// <summary>
/// Smoke tests validating that W3C trace-context (traceparent/tracestate) is propagated
/// from an incoming gRPC call handled by node A to an outgoing gRPC call to node B.
/// </summary>
public sealed class CorrelationSmokeTests : SmokeTestBase
{
    private const string TraceParentHeader = "traceparent";
    private const string TraceStateHeader = "tracestate";

    /// <summary>
    /// Starts two nodes (A,B). Sends a gRPC insert to A for a key owned by B with a custom traceparent header.
    /// Verifies that node B's gRPC server received the same traceparent in its request metadata.
    /// </summary>
    /// <returns>A Task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task TraceContextFlowsFromGrpcToGrpcAcrossNodes()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();

        var peers = BuildClusterPeers(("A", urlA), ("B", urlB));

        var capture = new CapturingHeadersInterceptor();

        await using var nodeA = await StartNodeAsync(urlA, peers, cancellationToken: DefaultCancellationToken);
        await using var nodeB = await StartNodeAsync(
            urlB,
            peers,
            configureGrpc: static o => o.Interceptors.Add<CapturingHeadersInterceptor>(),
            servicesConfigure: services => services.AddSingleton(capture),
            cancellationToken: DefaultCancellationToken);

        var key = new TestKeyOwnerHelper(["A", "B"]).FindKeyOwnedBy("default", "B", "correlation");

        using var activity = new Activity("test");
        _ = activity.SetIdFormat(ActivityIdFormat.W3C);
        _ = activity.Start();
        var traceparent = activity.Id;
        var tracestate = activity.TraceStateString;

        using var channel = CreateGrpcChannel(nodeA.Address);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { TraceParentHeader, traceparent! } };
        if (!string.IsNullOrEmpty(tracestate))
            headers.Add(TraceStateHeader, tracestate);

        _ = await client.TrySetAsync(
            new TrySetRequest
            {
                CacheName = "default",
                Key = key,
                Entry = new CacheEntry<object?> { Value = "value", Version = 1 }.MapToProto(),
            },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        await Task.Delay(50, DefaultCancellationToken);

        var last = capture.LastRequestHeaders;
        Assert.NotNull(last);
        var gotTp = last.GetValue(TraceParentHeader);
        Assert.False(string.IsNullOrEmpty(gotTp));

        var expectedTraceId = traceparent!.Split('-')[1];
        var gotTraceId = gotTp.Split('-')[1];
        Assert.Equal(expectedTraceId, gotTraceId);
    }
}
