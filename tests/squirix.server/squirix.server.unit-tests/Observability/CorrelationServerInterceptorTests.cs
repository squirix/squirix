using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Unit tests for inbound correlation handling in <see cref="ServerInterceptor" />.</summary>
[Immutable]
public sealed class CorrelationServerInterceptorTests
{
    /// <summary>Verifies empty or malformed inbound correlation headers are ignored instead of failing the request.</summary>
    [Test]
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
            static async (_, _) =>
            {
                var activity = await Assert.That(Activity.Current).IsNotNull();
                return activity.TraceId.ToString();
            });

        _ = await Assert.That(string.IsNullOrEmpty(observedTraceId)).IsFalse();
    }

    /// <summary>Verifies the server interceptor creates an activity when no incoming correlation headers exist.</summary>
    [Test]
    public async Task ServerInterceptorCreatesActivityAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
        var interceptor = CreateInterceptor();
        var observedTraceId = await interceptor.UnaryServerHandler(
            "request",
            new TestServerCallContext(),
            static async (_, _) =>
            {
                var activity = await Assert.That(Activity.Current).IsNotNull();
                return activity.TraceId.ToString();
            });

        _ = await Assert.That(string.IsNullOrEmpty(observedTraceId)).IsFalse();
    }

    /// <summary>Verifies an incoming valid traceparent propagates the trace id onto the server activity.</summary>
    [Test]
    public async Task ServerPropagatesTraceParentAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
        using var clientActivity = ActivitySourceHolder.StartClient("/Test.Test/Unary");
        _ = await Assert.That(clientActivity).IsNotNull();
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
            static async (_, _) =>
            {
                var activity = await Assert.That(Activity.Current).IsNotNull();
                return new CorrelationObservation(activity.TraceId.ToString(), activity.TraceStateString);
            });

        _ = await Assert.That(observed.TraceId).IsEqualTo(clientActivity.TraceId.ToString());
        _ = await Assert.That(observed.TraceStateString).IsEqualTo("vendor=value");
    }

    /// <summary>Verifies interceptor scope disposal restores the previous ambient activity after the call completes.</summary>
    [Test]
    public async Task ServerRestoresPreviousActivityAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener(true);
        using var outer = ActivitySourceHolder.StartInternal("outer");
        _ = await Assert.That(outer).IsNotNull();
        var interceptor = CreateInterceptor();
        var capture = new ActivityCapture();

        _ = await interceptor.UnaryServerHandler("request", new TestServerCallContext(), capture.HandleAsync);

        _ = await Assert.That(capture.Inside).IsNotNull();
        _ = await Assert.That(capture.Inside).IsNotSameReferenceAs(outer);
        _ = await Assert.That(Activity.Current).IsSameReferenceAs(outer);
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
