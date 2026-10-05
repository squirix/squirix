using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

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
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TraceContextFlowsAcrossGrpcNodes(CancellationToken cancellationToken)
    {
        var capture = new CapturingHeadersInterceptor();
        var servicesConfigure = new CaptureServicesConfigure(capture);

        await using var cluster = await StartClusterAsync(
            "A",
            "B",
            node => string.Equals(node, "B", StringComparison.Ordinal)
                ? new BlackBoxStartOptions
                {
                    ConfigureGrpc = static o => o.Interceptors.Add<CapturingHeadersInterceptor>(),
                    ServicesConfigure = servicesConfigure.Apply,
                }
                : new BlackBoxStartOptions(),
            cancellationToken);

        var key = TestKeyOwnerHelper.SmokeTwoNode.FindKeyOwnedBy("default", "B", "correlation");

        using var activity = new Activity("test");
        _ = activity.SetIdFormat(ActivityIdFormat.W3C);
        _ = activity.Start();
        var traceparent = activity.Id;
        var tracestate = activity.TraceStateString;

        using var channel = CreateGrpcChannel(cluster["A"].Uri);
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
        var last = capture.LastRequestHeaders;
        _ = await Assert.That(last).IsNotNull();
        var gotTp = last.GetValue(TraceParentHeader);
        _ = await Assert.That(string.IsNullOrEmpty(gotTp)).IsFalse();

        var expectedTraceId = TraceIdFromTraceparent(traceparent!);
        var gotTraceId = TraceIdFromTraceparent(gotTp!);
        _ = await Assert.That(gotTraceId).IsEqualTo(expectedTraceId);
    }

    /// <summary>Builds a wire entry carrying one string value in the shared scalar envelope.</summary>
    /// <param name="value">String payload.</param>
    /// <returns>The wire entry.</returns>
    private static CacheEntryWire CreateStringEntry(string value)
    {
        var envelope = new Struct();
        envelope.Fields.Add(ValueEnvelope.ScalarEnvelopeKey, Value.ForString(value));
        return new CacheEntryWire { Value = envelope };
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

    [Immutable]
    private sealed class CaptureServicesConfigure
    {
        private readonly CapturingHeadersInterceptor _capture;

        internal CaptureServicesConfigure(CapturingHeadersInterceptor capture)
        {
            _capture = capture;
            Apply = ApplyCore;
        }

        internal Action<IServiceCollection> Apply { get; }

        private void ApplyCore(IServiceCollection services) => services.AddSingleton(_capture);
    }

    /// <summary>
    /// Test-only server-side gRPC interceptor that captures the latest request metadata headers of the add call under test,
    /// so a background call of the node cannot overwrite them.
    /// Useful for asserting trace-context propagation in smoke tests.
    /// </summary>
    private sealed class CapturingHeadersInterceptor : Interceptor
    {
        private volatile Metadata? _last;

        /// <summary>Gets the request metadata headers of the last add call.</summary>
        internal Metadata? LastRequestHeaders => _last;

        /// <inheritdoc />
        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            if (context.Method.EndsWith("/TryAddEntry", StringComparison.Ordinal))
                _last = context.RequestHeaders;

            return base.UnaryServerHandler(request, context, continuation);
        }
    }
}
