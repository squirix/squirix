using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using Xunit;

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
    [Fact]
    public async Task TraceContextFlowsAcrossGrpcNodes()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();

        var peers = BuildClusterPeers([("A", uriA), ("B", uriB)]);

        var capture = new CapturingHeadersInterceptor();
        var servicesConfigure = new CaptureServicesConfigure(capture);

        await using var nodeA = await StartNodeAsync(uriA, peers, cancellationToken: DefaultCancellationToken);
        await using var nodeB = await StartNodeAsync(
            uriB,
            peers,
            new SmokeNodeStartOptions
            {
                ConfigureGrpc = static o => o.Interceptors.Add<CapturingHeadersInterceptor>(),
                ServicesConfigure = servicesConfigure.Apply,
            },
            DefaultCancellationToken);

        var key = TestKeyOwnerHelper.SmokeTwoNode.FindKeyOwnedBy("default", "B", "correlation");

        using var activity = new Activity("test");
        _ = activity.SetIdFormat(ActivityIdFormat.W3C);
        _ = activity.Start();
        var traceparent = activity.Id;
        var tracestate = activity.TraceStateString;

        using var channel = CreateGrpcChannel(nodeA.Uri);
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
                Entry = new NodeCacheEntry<object?> { Value = "value", Version = 1 }.MapToProto(),
            },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        await Task.Delay(50, DefaultCancellationToken);

        var last = capture.LastRequestHeaders;
        Assert.NotNull(last);
        var gotTp = last.GetValue(TraceParentHeader);
        Assert.False(string.IsNullOrEmpty(gotTp));

        var expectedTraceId = TraceIdFromTraceparent(traceparent!);
        var gotTraceId = TraceIdFromTraceparent(gotTp);
        Assert.Equal(expectedTraceId, gotTraceId);
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
    /// Test-only server-side gRPC interceptor that captures the latest request metadata headers.
    /// Useful for asserting trace-context propagation in smoke tests.
    /// </summary>
    private sealed class CapturingHeadersInterceptor : Interceptor
    {
        private volatile Metadata? _last;

        /// <summary>Gets the last captured request metadata headers.</summary>
        internal Metadata? LastRequestHeaders => _last;

        /// <inheritdoc />
        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            _last = context.RequestHeaders;
            return base.UnaryServerHandler(request, context, continuation);
        }
    }
}
