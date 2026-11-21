using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
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
    /// <summary>
    /// Starts two nodes (A,B). Sends a gRPC insert to A for a key owned by B with a custom traceparent header.
    /// Verifies that node B's gRPC server received the same traceparent in its request metadata.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TraceContextFlowsAcrossGrpcNodes(CancellationToken cancellationToken)
    {
        string? received = null;
        string? origin = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, "Squirix", StringComparison.Ordinal),
            Sample = static (ref _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        var entry = new BinaryCallObserver(_ => Volatile.Write(ref origin, Activity.Current?.Id));

        var observer = new BinaryCallObserver(http =>
        {
            if (http.Path.EndsWith("/TryAddEntry", StringComparison.Ordinal))
                Volatile.Write(ref received, http.TraceParent);
        });
        await using var cluster = await StartClusterAsync(
            "A",
            "B",
            node => string.Equals(node, "B", StringComparison.Ordinal)
                ? new BlackBoxStartOptions { ServicesConfigure = services => services.AddSingleton(observer) }
                : new BlackBoxStartOptions { ServicesConfigure = services => services.AddSingleton(entry) },
            cancellationToken);

        var key = TestKeyOwnerHelper.SmokeTwoNode.FindKeyOwnedBy("default", "B", "correlation");

        using var channel = CreateBinaryChannel(cluster["A"].Uri);
        var client = new CacheWireClient(channel);
        _ = await client.AddEntryIfAbsentAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = CreateStringEntry("value"),
            },
            cancellationToken: cancellationToken);

        // The forwarded call reached node B before node A answered, so its headers are already captured.
        var gotTp = Volatile.Read(ref received);
        _ = await Assert.That(string.IsNullOrEmpty(gotTp)).IsFalse();

        var expectedTraceId = TraceIdFromTraceparent(Volatile.Read(ref origin)!);
        var gotTraceId = TraceIdFromTraceparent(gotTp!);
        _ = await Assert.That(gotTraceId).IsEqualTo(expectedTraceId);
    }

    /// <summary>Builds a wire entry carrying one string value.</summary>
    /// <param name="value">String payload.</param>
    /// <returns>The wire entry.</returns>
    private static CacheEntryWire CreateStringEntry(string value) => new() { Value = new CacheValue { StringValue = value } };

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
