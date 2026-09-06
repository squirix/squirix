using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Tests trace header propagation on outbound unary calls through <see cref="ClientInterceptor" />.
/// </summary>
[Immutable]
public sealed class CorrelationClientInterceptorTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies an active activity id is written to gRPC metadata as <c language="csharp">traceparent</c>.
    /// </summary>
    [Fact]
    public void InterceptorAddsTraceParentFromActivity()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var capture = new HeaderCapture();
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        _ = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", default), capture.OnContinueAsync);

        Assert.NotNull(capture.Headers);

        Assert.Contains(capture.Headers, static entry => string.Equals(entry.Key, "traceparent", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(entry.Value));
    }

    /// <summary>
    /// Verifies an existing <c language="csharp">traceparent</c> entry is replaced when the current activity supplies a different id.
    /// </summary>
    [Fact]
    public void InterceptorReplacesTraceParentHeader()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var capture = new HeaderCapture();
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var staleHeaders = new Metadata { { "traceparent", "00-stale-stale-00" } };

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        _ = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(staleHeaders)), capture.OnContinueAsync);

        Assert.NotNull(capture.Headers);
        var values = CollectHeaderValues(capture.Headers, "traceparent");

        _ = Assert.Single(values);
        Assert.NotEqual("00-stale-stale-00", values[0], StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies an existing <c language="csharp">tracestate</c> entry is replaced from the current activity state.
    /// </summary>
    [Fact]
    public void InterceptorReplacesTraceStateHeader()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var capture = new HeaderCapture();
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var staleHeaders = new Metadata { { "tracestate", "old=state" } };
        using var activity = ActivitySourceHolder.StartClient(method.FullName);
        Assert.NotNull(activity);
        activity.TraceStateString = "vendor=value";

        _ = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(staleHeaders)), capture.OnContinueAsync);

        Assert.NotNull(capture.Headers);
        var values = CollectHeaderValues(capture.Headers, "tracestate");

        _ = Assert.Single(values);
        Assert.Equal("vendor=value", values[0]);
    }

    /// <summary>Ensures the interceptor clones caller headers instead of mutating them.</summary>
    [Fact]
    public void InterceptorLeavesCallerHeadersUnmodified()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var capture = new HeaderCapture();
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var callerHeaders = new Metadata { { "x-shared", "yes" }, { "x-binary-bin", [1, 2, 3] } };
        var before = SnapshotEntries(callerHeaders);

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        var call = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(callerHeaders)), capture.OnContinueAsync);

        Assert.NotNull(capture.Headers);
        Assert.Contains(capture.Headers, static entry => string.Equals(entry.Key, "traceparent", StringComparison.OrdinalIgnoreCase));
        AssertEntriesEqual(before, SnapshotEntries(callerHeaders));
        Assert.DoesNotContain(callerHeaders, static entry => string.Equals(entry.Key, "traceparent", StringComparison.OrdinalIgnoreCase));

        call.Dispose();
    }

    /// <summary>Ensures concurrent calls sharing one metadata instance do not bleed headers.</summary>
    [Fact]
    public async Task SharedMetadataCallsDoNotBleedAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();
        var sharedHeaders = new Metadata { { "x-shared", "yes" } };
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        const int callCount = 32;
        var states = new ConcurrentCallState[callCount];
        for (var index = 0; index < states.Length; index++)
            states[index] = new ConcurrentCallState(interceptor, method, sharedHeaders, gate.Task, new HeaderCapture());

        var tasks = new Task[callCount];
        for (var index = 0; index < tasks.Length; index++)
            tasks[index] = StartCallAsync(states[index]);

        _ = gate.TrySetResult();
        await Task.WhenAll(tasks);

        AssertEntriesEqual(["x-shared=yes"], SnapshotEntries(sharedHeaders));
        for (var index = 0; index < states.Length; index++)
        {
            var headers = states[index].Capture.Headers;
            Assert.NotNull(headers);
            _ = Assert.Single(CollectHeaderValues(headers, "traceparent"));
            Assert.Contains(headers, static entry => string.Equals(entry.Key, "x-shared", StringComparison.OrdinalIgnoreCase));
        }

        return;

        Task StartCallAsync(ConcurrentCallState state)
        {
            return Task.Factory.StartNew(
                () => InvokeCallAsync(state, DefaultCancellationToken),
                DefaultCancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }
    }

    /// <summary>Ensures an early outer dispose does not recycle the in-flight header bag.</summary>
    [Fact]
    public async Task EarlyDisposeKeepsInflightHeadersAsync()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        var capture = new PendingCallCapture();
        var interceptor = CreateInterceptor();
        var method = CreateUnaryStringMethod();

        using var activity = ActivitySourceHolder.StartClient(method.FullName);

        Assert.NotNull(activity);

        var call = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", default), capture.OnContinueAsync);

        var headers = capture.Headers;
        Assert.NotNull(headers);
        _ = Assert.Single(CollectHeaderValues(headers, "traceparent"));

        // Early dispose before the transport completes: the captured bag must stay intact.
        call.Dispose();

        headers = capture.Headers;
        Assert.NotNull(headers);
        _ = Assert.Single(CollectHeaderValues(headers, "traceparent"));

        capture.Complete("ok");
        var response = await call.ResponseAsync;
        Assert.Equal("ok", response);

        call.Dispose();
    }

    private static async Task InvokeCallAsync(ConcurrentCallState state, CancellationToken cancellationToken)
    {
        await state.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        using var activity = ActivitySourceHolder.StartClient(state.Method.FullName);
        var call = state.Interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(state.Method, "localhost", new CallOptions(state.SharedHeaders)), state.Capture.OnContinueAsync);
        try
        {
            _ = await call.ResponseAsync.ConfigureAwait(false);
        }
        finally
        {
            call.Dispose();
        }
    }

    private static void AssertEntriesEqual(List<string> expected, List<string> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var index = 0; index < expected.Count; index++)
            Assert.Equal(expected[index], actual[index]);
    }

    private static List<string> SnapshotEntries(Metadata headers)
    {
        var entries = new List<string>();
        for (var index = 0; index < headers.Count; index++)
        {
            var entry = headers[index];
            entries.Add(entry.IsBinary ? entry.Key + "=" + Convert.ToBase64String(entry.ValueBytes) : entry.Key + "=" + entry.Value);
        }

        return entries;
    }

    private static List<string> CollectHeaderValues(Metadata headers, string key)
    {
        var values = new List<string>();
        for (var index = 0; index < headers.Count; index++)
        {
            var entry = headers[index];
            if (!string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase))
                continue;

            values.Add(entry.Value);
        }

        return values;
    }

    private static ClientInterceptor CreateInterceptor() => new(NullLogger<ClientInterceptor>.Instance, "n1");

    private static Method<string, string> CreateUnaryStringMethod()
    {
        var marshaller = Marshallers.Create(static value => Encoding.UTF8.GetBytes(value), static bytes => Encoding.UTF8.GetString(bytes));
        return new Method<string, string>(MethodType.Unary, "Test", "Echo", marshaller, marshaller);
    }

    private sealed class HeaderCapture
    {
        internal Metadata? Headers { get; private set; }

        internal AsyncUnaryCall<string> OnContinueAsync(string request, ClientInterceptorContext<string, string> context)
        {
            _ = request;
            Headers = SnapshotHeaders(context.Options.Headers);
            return CreateCompletedUnaryCallAsync("ok");
        }

        private static AsyncUnaryCall<string> CreateCompletedUnaryCallAsync(string response)
        {
            return new AsyncUnaryCall<string>(
                Task.FromResult(response),
                Task.FromResult(Metadata.Empty),
                static () => Status.DefaultSuccess,
                static () => Metadata.Empty,
                static () => { });
        }

        private static Metadata? SnapshotHeaders(Metadata? headers)
        {
            if (headers == null)
                return null;

            var snapshot = new Metadata();
            for (var index = 0; index < headers.Count; index++)
            {
                var entry = headers[index];
                if (entry.IsBinary)
                    snapshot.Add(entry.Key, entry.ValueBytes);
                else
                    snapshot.Add(entry.Key, entry.Value);
            }

            return snapshot;
        }
    }

    private sealed class PendingCallCapture
    {
        private readonly TaskCompletionSource<string> _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Metadata? Headers { get; private set; }

        internal void Complete(string response) => _ = _gate.TrySetResult(response);

        internal AsyncUnaryCall<string> OnContinueAsync(string request, ClientInterceptorContext<string, string> context)
        {
            _ = request;

            // Keep the live transport-visible bag (not a copy) to observe pool recycling.
            Headers = context.Options.Headers;
            return new AsyncUnaryCall<string>(
                _gate.Task,
                Task.FromResult(Metadata.Empty),
                static () => Status.DefaultSuccess,
                static () => Metadata.Empty,
                static () => { });
        }
    }

    private sealed class ConcurrentCallState
    {
        internal ConcurrentCallState(ClientInterceptor interceptor, Method<string, string> method, Metadata sharedHeaders, Task gate, HeaderCapture capture)
        {
            Interceptor = interceptor;
            Method = method;
            SharedHeaders = sharedHeaders;
            Gate = gate;
            Capture = capture;
        }

        internal HeaderCapture Capture { get; }

        internal Task Gate { get; }

        internal ClientInterceptor Interceptor { get; }

        internal Method<string, string> Method { get; }

        internal Metadata SharedHeaders { get; }
    }
}
