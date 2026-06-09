using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
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
    /// Starts two nodes (A,B). Sends a REST PUT to A for a key owned by B with a custom traceparent header.
    /// Verifies that node B's gRPC server received the same traceparent in its request metadata.
    /// </summary>
    /// <returns>A Task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task TraceContextFlowsFromRestToGrpcAcrossNodes()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();

        var peers = new[]
        {
            new Peer { NodeId = "A", Url = urlA },
            new Peer { NodeId = "B", Url = urlB },
        };

        // Capturing interceptor will run on node B to observe incoming gRPC metadata.
        var capture = new CapturingHeadersInterceptor();

        await using var nodeA = await StartNodeAsync(urlA, peers, cancellationToken: DefaultCancellationToken);
        await using var nodeB = await StartNodeAsync(
            urlB,
            peers,
            configureGrpc: static o => o.Interceptors.Add<CapturingHeadersInterceptor>(),
            servicesConfigure: services => services.AddSingleton(capture),
            cancellationToken: DefaultCancellationToken);

        // Find a key owned by B to force a cross-node RPC from A -> B
        var key = await FindKeyOwnedByAsync(nodeA.Address, "B");

        // Create an Activity so we can get a valid W3C traceparent string.
        using var activity = new Activity("test");
        _ = activity.SetIdFormat(ActivityIdFormat.W3C);
        _ = activity.Start();
        var traceparent = activity.Id; // W3C formatted
        var tracestate = activity.TraceStateString; // may be null

        // Perform a REST write to node A with W3C headers to trigger A -> B gRPC routing.
        using var request = new HttpRequestMessage(HttpMethod.Put, $"{nodeA.Address}/api/v1/cache/{Uri.EscapeDataString(key)}");
        request.Content = new StringContent("{\"Value\":\"value\",\"Version\":1}", Encoding.UTF8, "application/json");
        request.Version = HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        request.Headers.Add(TraceParentHeader, traceparent!);
        if (!string.IsNullOrEmpty(tracestate))
            request.Headers.Add(TraceStateHeader, tracestate);

        using var response = await HttpClient.SendAsync(request, DefaultCancellationToken);
        var responseBody = await response.Content.ReadAsStringAsync(DefaultCancellationToken);
        Assert.True(response.IsSuccessStatusCode, $"Expected successful REST write, got {(int)response.StatusCode}: {responseBody}");

        // Wait briefly to ensure interceptor captured the call.
        await Task.Delay(50, DefaultCancellationToken);

        var last = capture.LastRequestHeaders;
        Assert.NotNull(last);
        var gotTp = last.GetValue(TraceParentHeader);
        Assert.False(string.IsNullOrEmpty(gotTp));

        // Compare only trace-id portion (second segment) as span-id changes per hop
        var expectedTraceId = traceparent!.Split('-')[1];
        var gotTraceId = gotTp.Split('-')[1];
        Assert.Equal(expectedTraceId, gotTraceId);
    }

    /// <summary>
    /// Finds a key owned by the specified node by probing the /admin/owner endpoint.
    /// </summary>
    /// <param name="nodeAddress">Base address of the node to query.</param>
    /// <param name="expectedOwner">The desired owner node id.</param>
    /// <returns>A Task producing a key string owned by the expected node.</returns>
    private async Task<string> FindKeyOwnedByAsync(string nodeAddress, string expectedOwner)
    {
        for (var i = 0; i < 2000; i++)
        {
            var k = $"k{i:0000}";
            var who = await HttpClient.GetStringAsync(nodeAddress + "/admin/owner/" + Uri.EscapeDataString(k), DefaultCancellationToken);
            if (who.Contains($"\"owner\":\"{expectedOwner}\"", StringComparison.Ordinal))
                return k;
        }

        throw new InvalidOperationException($"Failed to find a key owned by {expectedOwner}");
    }
}
