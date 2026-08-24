using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Unit tests for inbound correlation handling in <see cref="ServerInterceptor" />.
/// </summary>
[Immutable]
public sealed class CorrelationServerInterceptorTests
{
    /// <summary>Verifies the server interceptor creates an activity when no incoming correlation headers exist.</summary>
    [Fact]
    public async Task ServerInterceptorCreatesActivityAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
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
    public async Task ServerIgnoresEmptyHeadersAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
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
    public async Task ServerPropagatesTraceParentAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
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
    public async Task ServerRestoresPreviousActivityAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
        using var outer = ActivitySourceHolder.StartInternal("outer");
        Assert.NotNull(outer);
        var interceptor = CreateInterceptor();
        var capture = new ActivityCapture();

        _ = await interceptor.UnaryServerHandler("request", new TestServerCallContext(), capture.HandleAsync);

        Assert.NotNull(capture.Inside);
        Assert.NotSame(outer, capture.Inside);
        Assert.Same(outer, Activity.Current);
    }

    private static ServerInterceptor CreateInterceptor() => new(NullLogger<ServerInterceptor>.Instance, "n1");

    [Immutable]
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
}
