using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Verifies which protocols reach the public cache listener.</summary>
public sealed class BinaryTransportSurfaceTests
{
    private static readonly SocketsHttpHandler LoopbackHandler = LoopbackHttp.CreateHandler();
    private static readonly HttpClient LoopbackClient = new(LoopbackHandler, false);

    /// <summary>The SDK reaches the cache via binary TLS while legacy gRPC has no public route.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinaryCacheAndHealthSharePublicPort(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(BinaryCacheAndHealthSharePublicPort), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var uri = cluster.GetUri("nodeA");
        var cache = await cluster.GetCacheAsync<string>("transport", cancellationToken: cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);
        var value = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(value.Value).IsEqualTo("value");

        using var health = await LoopbackClient.GetAsync(new Uri(uri, "/health"), cancellationToken);
        _ = await Assert.That(health.StatusCode).IsEqualTo(HttpStatusCode.OK);

        using var legacy = new HttpRequestMessage(HttpMethod.Post, new Uri(uri, "/squirix.cache.v2.SquirixCacheService/GetValue"));
        legacy.Version = HttpVersion.Version20;
        legacy.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        legacy.Content = new ByteArrayContent([0, 0, 0, 0, 0]);
        legacy.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
        using var reply = await LoopbackClient.SendAsync(legacy, cancellationToken);
        _ = await Assert.That(reply.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Preserves the caller's W3C trace across the entry node and a forwarded call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SdkTraceReachesEntryAndOwner(CancellationToken cancellationToken)
    {
        const string cacheName = "trace-context";
        const string traceState = "vendor=value";
        await using var cluster = await HostedCluster.StartTwoNodeAsync(nameof(SdkTraceReachesEntryAndOwner), cancellationToken: cancellationToken);
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy(cacheName, "nodeB", "forwarded-get");
        var owner = await cluster.GetCacheAsync<string>(cacheName, "nodeB", cancellationToken);
        await owner.SetAsync(key, "forwarded", cancellationToken: cancellationToken);
        var entry = await cluster.GetCacheAsync<string>(cacheName, "nodeA", cancellationToken);

        using var caller = new Activity("application");
        _ = caller.SetIdFormat(ActivityIdFormat.W3C);
        _ = caller.SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        caller.TraceStateString = traceState;
        _ = caller.Start();

        var spans = new ConcurrentQueue<Activity>();
        var traceId = caller.TraceId;
        using var listener = new ActivityListener();
        listener.ShouldListenTo = static source => string.Equals(source.Name, "Squirix", StringComparison.Ordinal);
        listener.Sample = static (ref _) => ActivitySamplingResult.AllDataAndRecorded;
        listener.ActivityStopped = span =>
        {
            if (span.Kind == ActivityKind.Server && span.TraceId == traceId && span.OperationName.EndsWith("/GetValue", StringComparison.Ordinal))
                spans.Enqueue(span);
        };
        ActivitySource.AddActivityListener(listener);

        _ = await Assert.That((await entry.GetValueAsync(key, cancellationToken)).Value).IsEqualTo("forwarded");
        var observed = spans.ToArray();
        _ = await Assert.That(observed.Length).IsEqualTo(2);
        _ = await Assert.That(observed[0].ParentSpanId).IsNotEqualTo(caller.SpanId);
        _ = await Assert.That(observed[1].ParentSpanId).IsEqualTo(caller.SpanId);
        _ = await Assert.That(observed[0].TraceStateString).IsEqualTo(traceState);
        _ = await Assert.That(observed[1].TraceStateString).IsEqualTo(traceState);
    }
}
