using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Observability;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Tests trace header propagation on outbound unary calls through <see cref="Correlation.ClientInterceptor" />.
/// </summary>
public sealed class CorrelationClientInterceptorTests
{
    /// <summary>
    /// Verifies an active activity id is written to gRPC metadata as <c>traceparent</c>.
    /// </summary>
    [Fact]
    public void ClientInterceptorAddsTraceParentFromCurrentActivity()
    {
        using var listener = CreateSquirixActivityListener();

        CallOptions? observed = null;
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        _ = interceptor.AsyncUnaryCall(
            "req",
            new ClientInterceptorContext<string, string>(method, "localhost", default),
            (req, ctx) =>
            {
                _ = req;
                observed = ctx.Options;

                return CreateCompletedUnaryCall("ok");
            });

        var options = Assert.NotNull(observed);
        var headers = options.Headers ?? [];

        Assert.Contains(headers, static entry => string.Equals(entry.Key, "traceparent", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(entry.Value));
    }

    /// <summary>
    /// Verifies an existing <c>traceparent</c> entry is replaced when the current activity supplies a different id.
    /// </summary>
    [Fact]
    public void ClientInterceptorReplacesExistingTraceParentHeader()
    {
        using var listener = CreateSquirixActivityListener();

        CallOptions? observed = null;
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var staleHeaders = new Metadata { { "traceparent", "00-stale-stale-00" } };

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        _ = interceptor.AsyncUnaryCall(
            "req",
            new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(staleHeaders)),
            (req, ctx) =>
            {
                _ = req;
                observed = ctx.Options;

                return CreateCompletedUnaryCall("ok");
            });

        var options = Assert.NotNull(observed);
        var values = (options.Headers ?? []).Where(static entry => string.Equals(entry.Key, "traceparent", StringComparison.OrdinalIgnoreCase)).Select(static entry => entry.Value)
                                            .ToArray();

        _ = Assert.Single(values);
        Assert.NotEqual("00-stale-stale-00", values[0]);
    }

    /// <summary>
    /// Verifies an existing <c>tracestate</c> entry is replaced from the current activity state.
    /// </summary>
    [Fact]
    public void ClientInterceptorReplacesExistingTraceStateHeader()
    {
        using var listener = CreateSquirixActivityListener();

        CallOptions? observed = null;
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var staleHeaders = new Metadata { { "tracestate", "old=state" } };
        using var activity = ActivitySourceHolder.StartClient(method.FullName);
        Assert.NotNull(activity);
        activity.TraceStateString = "vendor=value";

        _ = interceptor.AsyncUnaryCall(
            "req",
            new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(staleHeaders)),
            (req, ctx) =>
            {
                _ = req;
                observed = ctx.Options;
                return CreateCompletedUnaryCall("ok");
            });

        var options = Assert.NotNull(observed);
        var values = (options.Headers ?? []).Where(static entry => string.Equals(entry.Key, "tracestate", StringComparison.OrdinalIgnoreCase)).Select(static entry => entry.Value)
                                            .ToArray();
        _ = Assert.Single(values);
        Assert.Equal("vendor=value", values[0]);
    }

    private static AsyncUnaryCall<string> CreateCompletedUnaryCall(string response)
    {
        return new AsyncUnaryCall<string>(
            Task.FromResult(response),
            Task.FromResult(Metadata.Empty),
            static () => Status.DefaultSuccess,
            static () => Metadata.Empty,
            static () => { });
    }

    private static Correlation.ClientInterceptor CreateInterceptor()
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "c",
            NodeId = "n1",
            Peers = [],
            Url = "https://localhost",
        };

        return new Correlation.ClientInterceptor(NullLogger<Correlation.ClientInterceptor>.Instance, cluster);
    }

    private static ActivityListener CreateSquirixActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.OrdinalIgnoreCase),
            Sample = static (ref _) => ActivitySamplingResult.AllData,
        };

        ActivitySource.AddActivityListener(listener);

        return listener;
    }

    private static Method<string, string> CreateUnaryStringMethod()
    {
        var marshaller = Marshallers.Create(static value => Encoding.UTF8.GetBytes(value), static bytes => Encoding.UTF8.GetString(bytes));
        return new Method<string, string>(MethodType.Unary, "Test", "Echo", marshaller, marshaller);
    }
}
