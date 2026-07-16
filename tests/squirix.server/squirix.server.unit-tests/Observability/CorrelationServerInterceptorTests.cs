using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Node.Observability;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Unit tests for inbound correlation handling in <see cref="ServerInterceptor" />.
/// </summary>
public sealed class CorrelationServerInterceptorTests
{
    /// <summary>Verifies the server interceptor creates an activity when no incoming correlation headers exist.</summary>
    [Fact]
    public async Task ServerInterceptorCreatesActivityHeadersAreMissing()
    {
        using var listener = CreateSquirixActivityListener();
        var interceptor = CreateInterceptor();
        var observedTraceId = await interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static (_, _) =>
            {
                Assert.NotNull(Activity.Current);
                return Task.FromResult(Activity.Current.TraceId.ToString());
            });

        Assert.False(string.IsNullOrEmpty(observedTraceId));
    }

    /// <summary>Verifies empty or malformed inbound correlation headers are ignored instead of failing the request.</summary>
    [Fact]
    public async Task ServerInterceptorIgnoresEmptyCorrelationHeaders()
    {
        using var listener = CreateSquirixActivityListener();
        var interceptor = CreateInterceptor();
        var headers = new Metadata
        {
            { "traceparent", "not-a-valid-traceparent" },
            { "tracestate", "vendor=value" },
        };

        var observedTraceId = await interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(headers),
            static (_, _) =>
            {
                Assert.NotNull(Activity.Current);
                return Task.FromResult(Activity.Current.TraceId.ToString());
            });

        Assert.False(string.IsNullOrEmpty(observedTraceId));
    }

    /// <summary>Verifies an incoming valid traceparent propagates the trace id onto the server activity.</summary>
    [Fact]
    public async Task ServerInterceptorPropagatesIncomingTraceParent()
    {
        using var listener = CreateSquirixActivityListener();
        using var clientActivity = ActivitySourceHolder.StartClient("/Test.Test/Unary");
        Assert.NotNull(clientActivity);
        clientActivity.TraceStateString = "vendor=value";

        var interceptor = CreateInterceptor();
        var headers = new Metadata
        {
            { "traceparent", clientActivity.Id! },
            { "tracestate", clientActivity.TraceStateString },
        };

        var observed = await interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(headers),
            static (_, _) =>
            {
                Assert.NotNull(Activity.Current);
                return Task.FromResult(new CorrelationObservation(Activity.Current.TraceId.ToString(), Activity.Current.TraceStateString));
            });

        Assert.Equal(clientActivity.TraceId.ToString(), observed.TraceId);
        Assert.Equal("vendor=value", observed.TraceStateString);
    }

    /// <summary>Verifies interceptor scope disposal restores the previous ambient activity after the call completes.</summary>
    [Fact]
    public async Task ServerInterceptorRestoresPreviousActivityAfterCall()
    {
        using var listener = CreateSquirixActivityListener();
        using var outer = ActivitySourceHolder.StartInternal("outer");
        Assert.NotNull(outer);
        var interceptor = CreateInterceptor();
        var capture = new ActivityCapture();

        _ = await interceptor.UnaryServerHandler("request", new TestServerCallContext(), capture.HandleAsync);

        Assert.NotNull(capture.Inside);
        Assert.NotSame(outer, capture.Inside);
        Assert.Same(outer, Activity.Current);
    }

    private static Correlation.ServerInterceptor CreateInterceptor()
    {
        var cluster = new ClusterConfig([])
        {
            ClusterId = "c",
            NodeId = "n1",
            Uri = new Uri("https://localhost"),
        };

        return new Correlation.ServerInterceptor(NullLogger<Correlation.ServerInterceptor>.Instance, cluster);
    }

    private static ActivityListener CreateSquirixActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.OrdinalIgnoreCase),
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            SampleUsingParentId = static (ref _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed record CorrelationObservation(string TraceId, string? TraceStateString);

    private sealed class ActivityCapture
    {
        internal Activity? Inside { get; private set; }

        internal Task<string> HandleAsync(string request, ServerCallContext context)
        {
            _ = request;
            _ = context;
            Inside = Activity.Current;
            return Task.FromResult("ok");
        }
    }

    private sealed class TestServerCallContext : ServerCallContext
    {
        internal TestServerCallContext(Metadata? headers = null)
        {
            RequestHeadersCore = headers ?? [];
        }

        protected override AuthContext AuthContextCore => new(null, []);

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override string HostCore => "localhost";

        protected override string MethodCore => "/Test.Test/Unary";

        protected override string PeerCore => "ipv4:127.0.0.1:5001";

        protected override Metadata RequestHeadersCore { get; }

        protected override Metadata ResponseTrailersCore => [];

        protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
