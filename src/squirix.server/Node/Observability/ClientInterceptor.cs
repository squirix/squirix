using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Client-side interceptor for outbound trace context and logging scope correlation.</summary>
internal sealed class ClientInterceptor : Interceptor
{
    private readonly ILogger<ClientInterceptor> _log;
    private readonly string _nodeId;

    internal ClientInterceptor(ILogger<ClientInterceptor> log, string nodeId)
    {
        _log = log;
        _nodeId = nodeId;
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var callOptions = AttachTraceHeaders(context.Options, context.Method.FullName, out var ownedActivity);
        var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
        var scope = Correlation.BeginStandardScope(_log, _nodeId, context.Method.FullName);
        var call = base.AsyncUnaryCall(request, updatedContext, continuation);
        return WrapUnaryCallAsync(scope, ownedActivity, call);
    }

    private static CallOptions AttachTraceHeaders(CallOptions options, string method, out Activity? ownedActivity)
    {
        var metadata = options.Headers ?? [];

        // Reuse the ambient Activity when present; otherwise start one owned by the outbound call.
        ownedActivity = null;
        var activity = Activity.Current;
        if (activity is null)
        {
            activity = ActivitySourceHolder.StartClient(method);
            ownedActivity = activity;
        }

        if (activity is null)
            return new CallOptions(metadata, options.Deadline, options.CancellationToken, options.WriteOptions, options.PropagationToken, options.Credentials);

        var traceParent = activity.Id;
        if (!string.IsNullOrEmpty(traceParent))
            Upsert(metadata, Correlation.TraceParentHeader, traceParent);

        var traceState = activity.TraceStateString;
        if (!string.IsNullOrEmpty(traceState))
            Upsert(metadata, Correlation.TraceStateHeader, traceState);

        return new CallOptions(metadata, ServerRpcDeadlineContext.EffectiveDeadline(options.Deadline), options.CancellationToken, options.WriteOptions, options.PropagationToken, options.Credentials);
    }

    private static void Upsert(Metadata metadata, string key, string value)
    {
        for (var i = 0; i < metadata.Count; i++)
        {
            if (!string.Equals(metadata[i].Key, key, StringComparison.Ordinal))
                continue;

            metadata.RemoveAt(i);
            break;
        }

        metadata.Add(new Metadata.Entry(key, value));
    }

    private static AsyncUnaryCall<TResponse> WrapUnaryCallAsync<TResponse>(IDisposable scope, Activity? ownedActivity, AsyncUnaryCall<TResponse> inner)
    {
        var disposed = 0;

        async Task<TResponse> ResponseAsync()
        {
            try
            {
#pragma warning disable VSTHRD003

                // Scope and owned client Activity must live until the outbound unary call completes.
                return await inner.ResponseAsync.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            }
            finally
            {
                DisposeOnce();
            }
        }

        return new AsyncUnaryCall<TResponse>(
            ResponseAsync(),
            inner.ResponseHeadersAsync,
            inner.GetStatus,
            inner.GetTrailers,
            () =>
            {
                DisposeOnce();
                inner.Dispose();
            });

        void DisposeOnce()
        {
            if (Interlocked.Exchange(ref disposed, 1) is not 0)
                return;
            scope.Dispose();
            ownedActivity?.Dispose();
        }
    }
}
