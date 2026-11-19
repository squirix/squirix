using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>
/// Endpoint integration tests validating that W3C trace-context (traceparent/tracestate) is propagated
/// from an incoming gRPC call handled by node A to an outgoing gRPC call to node B.
/// </summary>
public sealed class CorrelationTests : ServerSurfaceTestBase
{
    private const string TraceParentHeader = "traceparent";
    private const string TraceStateHeader = "tracestate";

    /// <summary>
    /// Starts two nodes (A,B). Sends a gRPC insert to A for a key owned by B with a custom traceparent header.
    /// Verifies that node B's gRPC server received the same traceparent in its request metadata.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TraceContextFlowsAcrossGrpcNodes(CancellationToken cancellationToken)
    {
        string? received = null;

        var observer = new BinaryCallObserver(http =>
        {
            if (http.Request.Path.Value?.EndsWith("/TryAddEntry", StringComparison.Ordinal) == true)
                Volatile.Write(ref received, http.Request.Headers[TraceParentHeader].ToString());
        });
        await using var cluster = await StartClusterAsync(
            "A",
            "B",
            node => string.Equals(node, "B", StringComparison.Ordinal)
                ? new BlackBoxStartOptions { ServicesConfigure = services => services.AddSingleton(observer) }
                : new BlackBoxStartOptions(),
            cancellationToken);

        var key = TestKeyOwnerHelper.SmokeTwoNode.FindKeyOwnedBy("default", "B", "correlation");

        using var activity = new Activity("test");
        _ = activity.SetIdFormat(ActivityIdFormat.W3C);
        _ = activity.Start();
        var traceparent = activity.Id;
        var tracestate = activity.TraceStateString;

        var endpoint = FindInternalEndpoint(cluster.Peers);
        var identity = cluster["B"].GetRequiredService<MtlsCertificate>();
        using var handler = TestCertificates.CreateMtlsHandler(identity.NodeCertificate!, identity.TrustAnchor!, "A");
        using var channel = GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions { HttpHandler = handler });
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { TraceParentHeader, traceparent! } };
        if (!string.IsNullOrEmpty(tracestate))
            headers.Add(TraceStateHeader, tracestate);

        _ = await client.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = CreateStringEntry("value"),
            },
            new CallOptions(headers, cancellationToken: cancellationToken));

        // The forwarded call reached node B before node A answered, so its headers are already captured.
        var gotTp = Volatile.Read(ref received);
        _ = await Assert.That(string.IsNullOrEmpty(gotTp)).IsFalse();

        var expectedTraceId = TraceIdFromTraceparent(traceparent!);
        var gotTraceId = TraceIdFromTraceparent(gotTp!);
        _ = await Assert.That(gotTraceId).IsEqualTo(expectedTraceId);
    }

    /// <summary>Builds a wire entry carrying one string value.</summary>
    /// <param name="value">String payload.</param>
    /// <returns>The wire entry.</returns>
    private static CacheEntryWire CreateStringEntry(string value) => new() { Value = new CacheValue { StringValue = value } };

    private static Uri FindInternalEndpoint(System.Collections.Generic.IReadOnlyList<ServerPeer> peers)
    {
        foreach (var peer in peers)
        {
            if (string.Equals(peer.NodeId, "A", StringComparison.Ordinal) && peer.InterNodeUri is { } uri)
                return uri;
        }

        throw new InvalidOperationException("Node A has no internal endpoint.");
    }

    private static string TraceIdFromTraceparent(string traceparent)
    {
        var span = traceparent.AsSpan();
        var firstDash = span.IndexOf('-');
        if (firstDash < 0)
            throw new InvalidOperationException("traceparent is missing a dash separator.");

        var secondDash = span[(firstDash + 1)..].IndexOf('-');
        if (secondDash < 0)
            throw new InvalidOperationException("traceparent is missing the trace-id segment.");

        secondDash += firstDash + 1;
        return traceparent[(firstDash + 1)..secondDash];
    }
}
