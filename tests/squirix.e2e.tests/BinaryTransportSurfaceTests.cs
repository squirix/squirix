using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport;
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
    public async Task ManagementDoesNotShareCacheListener(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(ManagementDoesNotShareCacheListener), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var uri = cluster.GetUri("nodeA");
        var management = cluster.Cluster["nodeA"].ManagementUri;
        _ = await Assert.That(uri.Port).IsNotEqualTo(management.Port);
        var cache = await cluster.GetCacheAsync<string>("transport", cancellationToken: cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);
        var value = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(value.Value).IsEqualTo("value");

        using var health = await LoopbackClient.GetAsync(new Uri(management, "/health"), cancellationToken);
        _ = await Assert.That(health.StatusCode).IsEqualTo(HttpStatusCode.OK);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<HttpRequestException>(
            LoopbackClient.GetAsync(new Uri(uri, "/health"), cancellationToken),
            TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);

        using var legacy = new HttpRequestMessage(HttpMethod.Post, new Uri(management, "/squirix.cache.v2.SquirixCacheService/GetValue"));
        legacy.Content = new ByteArrayContent([0, 0, 0, 0, 0]);
        legacy.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
        using var reply = await LoopbackClient.SendAsync(legacy, cancellationToken);
        _ = await Assert.That(reply.StatusCode).IsEqualTo(HttpStatusCode.MethodNotAllowed);
    }

    /// <summary>A management origin is unavailable to the SDK, which can still bootstrap on the binary listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SdkFallsBackFromManagementOrigin(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(SdkFallsBackFromManagementOrigin), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        var management = cluster.Cluster["nodeA"].ManagementUri;
        var options = new SquirixClientOptions();
        options.Endpoints.Add(management);
        var error = await NodeAsyncAssert.ThrowsAsync<SquirixTransportException, ISquirixClient>(
            SquirixClient.ConnectAsync(options, handler, cancellationToken),
            TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(CacheTransportStatusCode.FailedPrecondition);

        options.Endpoints.Add(cluster.GetUri("nodeA"));
        await using var client = await SquirixClient.ConnectAsync(options, handler, cancellationToken);
        var cache = await client.GetCacheAsync<string>("listener-fallback", cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("key", cancellationToken)).Value).IsEqualTo("value");
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
