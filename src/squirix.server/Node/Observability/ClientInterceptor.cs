using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

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
        var callOptions = AttachTraceHeaders(context.Options, context.Method.FullName, out var ownedActivity, out var rentedHeaders);
        var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
        var scope = Correlation.BeginStandardScope(_log, _nodeId, context.Method.FullName);
        var call = base.AsyncUnaryCall(request, updatedContext, continuation);
        return OutboundUnaryCallLease<TResponse>.WrapAsync(scope, ownedActivity, rentedHeaders, call);
    }

    private static CallOptions AttachTraceHeaders(
        CallOptions options,
        string method,
        out Activity? ownedActivity,
        out Metadata? rentedHeaders)
    {
        // Reuse the ambient Activity when present; otherwise start one owned by the outbound call.
        ownedActivity = null;
        rentedHeaders = null;
        var activity = Activity.Current;
        if (activity is null)
        {
            activity = ActivitySourceHolder.StartClient(method);
            ownedActivity = activity;
        }

        if (activity is null)
        {
            // No trace headers to attach — keep caller headers untouched (including null).
            return options;
        }

        Metadata metadata;
        if (options.Headers is null)
        {
            metadata = GrpcMetadataPool.Rent();
            rentedHeaders = metadata;
        }
        else
        {
            metadata = options.Headers;
        }

        var traceParent = activity.Id;
        if (!string.IsNullOrEmpty(traceParent))
            Upsert(metadata, Correlation.TraceParentHeader, traceParent);

        var traceState = activity.TraceStateString;
        if (!string.IsNullOrEmpty(traceState))
            Upsert(metadata, Correlation.TraceStateHeader, traceState);

        return new CallOptions(
            metadata,
            ServerRpcDeadlineContext.EffectiveDeadline(options.Deadline),
            options.CancellationToken,
            options.WriteOptions,
            options.PropagationToken,
            options.Credentials);
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

        metadata.Add(key, value);
    }

    /// <summary>Owns logging scope, optional client Activity, and rented metadata for an outbound unary call.</summary>
    /// <typeparam name="TResponse">Outbound unary response type.</typeparam>
    private sealed class OutboundUnaryCallLease<TResponse>
    {
        private readonly AsyncUnaryCall<TResponse> _inner;
        private readonly Activity? _ownedActivity;
        private readonly Metadata? _rentedHeaders;
        private readonly IDisposable _scope;
        private int _disposed;

        private OutboundUnaryCallLease(
            IDisposable scope,
            Activity? ownedActivity,
            Metadata? rentedHeaders,
            AsyncUnaryCall<TResponse> inner)
        {
            _scope = scope;
            _ownedActivity = ownedActivity;
            _rentedHeaders = rentedHeaders;
            _inner = inner;
        }

        internal static AsyncUnaryCall<TResponse> WrapAsync(
            IDisposable scope,
            Activity? ownedActivity,
            Metadata? rentedHeaders,
            AsyncUnaryCall<TResponse> inner)
        {
            var lease = new OutboundUnaryCallLease<TResponse>(scope, ownedActivity, rentedHeaders, inner);
            return new AsyncUnaryCall<TResponse>(lease.ResponseAsync(), inner.ResponseHeadersAsync, inner.GetStatus, inner.GetTrailers, lease.DisposeCall);
        }

        private void DisposeCall()
        {
            DisposeOnce();
            _inner.Dispose();
        }

        private void DisposeOnce()
        {
            if (Interlocked.Exchange(ref _disposed, 1) is not 0)
                return;

            _scope.Dispose();
            _ownedActivity?.Dispose();
            GrpcMetadataPool.Return(_rentedHeaders);
        }

        private async Task<TResponse> ResponseAsync()
        {
            try
            {
#pragma warning disable VSTHRD003

                // Scope, owned client Activity, and rented headers must live until the outbound unary call completes.
                return await _inner.ResponseAsync.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            }
            finally
            {
                DisposeOnce();
            }
        }
    }
}
