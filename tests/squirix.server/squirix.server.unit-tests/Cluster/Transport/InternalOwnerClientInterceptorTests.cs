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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for the internal owner-routing header interceptor.</summary>
[Immutable]
public sealed class InternalOwnerClientInterceptorTests : ServerUnitTestBase
{
    /// <summary>Ensures the interceptor clones caller headers instead of mutating them.</summary>
    [Test]
    public async Task InterceptorLeavesCallerHeadersUnmodified()
    {
        var capture = new HeaderCapture();
        var interceptor = new InternalOwnerClientInterceptor();
        var method = CreateUnaryStringMethod();
        var callerHeaders = new Metadata { { "x-shared", "yes" }, { "x-binary-bin", [1, 2, 3] } };
        var before = SnapshotEntries(callerHeaders);

        using var call = interceptor.AsyncUnaryCall(
            "req",
            new ClientInterceptorContext<string, string>(method, "localhost", new CallOptions(callerHeaders)),
            capture.OnContinueAsync);

        var headers = capture.Headers;
        _ = await Assert.That(headers).IsNotNull();
        _ = await Assert.That(headers).IsNotSameReferenceAs(callerHeaders);
        await AssertEntriesEqualAsync(before, SnapshotEntries(callerHeaders));
        _ = await Assert.That(callerHeaders).DoesNotContain(static entry => string.Equals(
            entry.Key,
            RemoteInvocationContract.InternalOwnerRpcHeaderName,
            StringComparison.Ordinal));

        var values = CollectHeaderValues(headers, RemoteInvocationContract.InternalOwnerRpcHeaderName);
        _ = await Assert.That(values).HasSingleItem();
        _ = await Assert.That(values[0]).IsEqualTo(RemoteInvocationContract.InternalOwnerRpcHeaderValue);
    }

    /// <summary>Ensures concurrent calls sharing one metadata instance do not bleed headers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedMetadataCallsDoNotBleedAsync(CancellationToken cancellationToken)
    {
        var interceptor = new InternalOwnerClientInterceptor();
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

        await AssertEntriesEqualAsync(["x-shared=yes"], SnapshotEntries(sharedHeaders));
        for (var index = 0; index < states.Length; index++)
        {
            var headers = states[index].Capture.Headers;
            _ = await Assert.That(headers).IsNotNull();
            _ = await Assert.That(headers).IsNotSameReferenceAs(sharedHeaders);
            var values = CollectHeaderValues(headers, RemoteInvocationContract.InternalOwnerRpcHeaderName);
            _ = await Assert.That(values).HasSingleItem();
            _ = await Assert.That(values[0]).IsEqualTo(RemoteInvocationContract.InternalOwnerRpcHeaderValue);
            _ = await Assert.That(headers).Contains(static entry => string.Equals(entry.Key, "x-shared", StringComparison.OrdinalIgnoreCase));
        }

        return;

        Task StartCallAsync(ConcurrentCallState state)
        {
            return Task.Factory.StartNew(() => InvokeCallAsync(state, cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
        }
    }

    private static async Task AssertEntriesEqualAsync(List<string> expected, List<string> actual)
    {
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (var index = 0; index < expected.Count; index++)
            _ = await Assert.That(actual[index]).IsEqualTo(expected[index]);
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

    private static Method<string, string> CreateUnaryStringMethod()
    {
        var marshaller = Marshallers.Create(static value => Encoding.UTF8.GetBytes(value), static bytes => Encoding.UTF8.GetString(bytes));
        return new Method<string, string>(MethodType.Unary, "Test", "Echo", marshaller, marshaller);
    }

    private static async Task InvokeCallAsync(ConcurrentCallState state, CancellationToken cancellationToken)
    {
        await state.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        using var call = state.Interceptor.AsyncUnaryCall(
            "req",
            new ClientInterceptorContext<string, string>(state.Method, "localhost", new CallOptions(state.SharedHeaders)),
            state.Capture.OnContinueAsync);
        _ = await call.ResponseAsync.ConfigureAwait(false);
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

    private sealed class ConcurrentCallState
    {
        internal ConcurrentCallState(InternalOwnerClientInterceptor interceptor, Method<string, string> method, Metadata sharedHeaders, Task gate, HeaderCapture capture)
        {
            Interceptor = interceptor;
            Method = method;
            SharedHeaders = sharedHeaders;
            Gate = gate;
            Capture = capture;
        }

        internal HeaderCapture Capture { get; }

        internal Task Gate { get; }

        internal InternalOwnerClientInterceptor Interceptor { get; }

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
