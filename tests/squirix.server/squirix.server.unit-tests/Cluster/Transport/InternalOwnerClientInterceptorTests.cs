using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for the internal owner-routing header interceptor.</summary>
[Immutable]
public sealed class InternalOwnerClientInterceptorTests : ServerUnitTestBase
{
    /// <summary>Ensures the interceptor clones caller headers instead of mutating them.</summary>
    [Fact]
    public void InterceptorLeavesCallerHeadersUnmodified()
    {
        var capture = new HeaderCapture();
        var interceptor = new ServiceRegistration.InternalOwnerClientInterceptor();
        var method = CreateUnaryStringMethod();
        var callerHeaders = new Metadata { { "x-shared", "yes" }, { "x-binary-bin", [1, 2, 3] } };
        var before = SnapshotEntries(callerHeaders);

        using var call = interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(callerHeaders)), capture.OnContinueAsync);

        var headers = capture.Headers;
        Assert.NotNull(headers);
        Assert.NotSame(callerHeaders, headers);
        AssertEntriesEqual(before, SnapshotEntries(callerHeaders));
        Assert.DoesNotContain(callerHeaders, static entry => string.Equals(entry.Key, RemoteInvocationContract.InternalOwnerRpcHeaderName, StringComparison.Ordinal));

        var values = CollectHeaderValues(headers, RemoteInvocationContract.InternalOwnerRpcHeaderName);
        _ = Assert.Single(values);
        Assert.Equal(RemoteInvocationContract.InternalOwnerRpcHeaderValue, values[0]);
    }

    /// <summary>Ensures concurrent calls sharing one metadata instance do not bleed headers.</summary>
    [Fact]
    public async Task SharedMetadataCallsDoNotBleedAsync()
    {
        var interceptor = new ServiceRegistration.InternalOwnerClientInterceptor();
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
            Assert.NotSame(sharedHeaders, headers);
            var values = CollectHeaderValues(headers, RemoteInvocationContract.InternalOwnerRpcHeaderName);
            _ = Assert.Single(values);
            Assert.Equal(RemoteInvocationContract.InternalOwnerRpcHeaderValue, values[0]);
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

    private static async Task InvokeCallAsync(ConcurrentCallState state, CancellationToken cancellationToken)
    {
        await state.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        using var call = state.Interceptor.AsyncUnaryCall("req", new ClientInterceptorContext<string, string>(state.Method, "localhost", new CallOptions(state.SharedHeaders)), state.Capture.OnContinueAsync);
        _ = await call.ResponseAsync.ConfigureAwait(false);
    }

    private static void AssertEntriesEqual(List<string> expected, List<string> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var index = 0; index < expected.Count; index++)
            Assert.Equal(expected[index], actual[index]);
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

    private static Method<string, string> CreateUnaryStringMethod()
    {
        var marshaller = Marshallers.Create(static value => Encoding.UTF8.GetBytes(value), static bytes => Encoding.UTF8.GetString(bytes));
        return new Method<string, string>(MethodType.Unary, "Test", "Echo", marshaller, marshaller);
    }

    private sealed class ConcurrentCallState
    {
        internal ConcurrentCallState(ServiceRegistration.InternalOwnerClientInterceptor interceptor, Method<string, string> method, Metadata sharedHeaders, Task gate, HeaderCapture capture)
        {
            Interceptor = interceptor;
            Method = method;
            SharedHeaders = sharedHeaders;
            Gate = gate;
            Capture = capture;
        }

        internal HeaderCapture Capture { get; }

        internal Task Gate { get; }

        internal ServiceRegistration.InternalOwnerClientInterceptor Interceptor { get; }

        internal Method<string, string> Method { get; }

        internal Metadata SharedHeaders { get; }
    }

    private sealed class HeaderCapture
    {
        internal Metadata? Headers { get; private set; }

        internal AsyncUnaryCall<string> OnContinueAsync(string request, ClientInterceptorContext<string, string> context)
        {
            _ = request;

            // Keep the live transport-visible bag (not a copy) to observe aliasing with the caller bag.
            Headers = context.Options.Headers;
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
    }
}
