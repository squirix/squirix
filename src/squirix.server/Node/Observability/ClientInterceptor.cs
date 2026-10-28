using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Client-side interceptor for outbound trace context propagation and the effective operation deadline.</summary>
internal sealed class ClientInterceptor : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var callOptions = AttachTraceHeaders(context.Options, context.Method.FullName, out var ownedActivity, out var rentedHeaders);
        if (ownedActivity == null && rentedHeaders == null)
        {
            // Nothing to release when the call completes: forward the call without a wrapper.
            return callOptions.Deadline == context.Options.Deadline
                ? continuation(request, context)
                : continuation(request, new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions));
        }

        var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
        var call = continuation(request, updatedContext);
        return OutboundUnaryCallLease<TResponse>.WrapAsync(ownedActivity, rentedHeaders, call);
    }

    private static CallOptions AttachTraceHeaders(CallOptions options, string method, out Activity? ownedActivity, out Metadata? rentedHeaders)
    {
        // The operation deadline reaches every forwarded call, whether or not trace headers are attached below.
        options = WithEffectiveDeadline(options);

        // Reuse the ambient Activity when present; otherwise start one owned by the outbound call.
        ownedActivity = null;
        rentedHeaders = null;
        var activity = Activity.Current;
        if (activity == null)
        {
            activity = ActivitySourceHolder.StartClient(method);
            ownedActivity = activity;
        }

        // No trace headers to attach — keep caller headers untouched (including null).
        if (activity == null)
            return options;

        // Clone caller headers into a freshly rented bag; never mutate options.Headers in place,
        // so a Metadata instance shared across calls cannot bleed trace headers or race.
        var metadata = GrpcMetadataPool.Rent();
        rentedHeaders = metadata;
        var callerHeaders = options.Headers;
        if (callerHeaders != null)
            GrpcMetadata.CopyInto(metadata, callerHeaders);

        var traceParent = activity.Id;
        if (!string.IsNullOrEmpty(traceParent))
            Upsert(metadata, Correlation.TraceParentHeader, traceParent);

        var traceState = activity.TraceStateString;
        if (!string.IsNullOrEmpty(traceState))
            Upsert(metadata, Correlation.TraceStateHeader, traceState);

        return new CallOptions(
            metadata,
            options.Deadline,
            options.CancellationToken,
            options.WriteOptions,
            options.PropagationToken,
            options.Credentials);
    }

    private static CallOptions WithEffectiveDeadline(CallOptions options)
    {
        var effective = ServerRpcDeadlineContext.EffectiveDeadline(options.Deadline);
        return effective == null || effective == options.Deadline ? options : options.WithDeadline(effective.Value);
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

    private static class GrpcMetadataPool
    {
        private static readonly ConcurrentBag<Metadata> Pool = [];

        /// <summary>Rents a cleared <see cref="Metadata" /> instance from the pool or allocates when empty.</summary>
        /// <returns>A reusable metadata bag owned by the caller until <see cref="Return" />.</returns>
        internal static Metadata Rent()
        {
            if (!Pool.TryTake(out var metadata))
                return [];
            metadata.Clear();
            return metadata;
        }

        /// <summary>Clears and returns a rented <see cref="Metadata" /> instance to the pool.</summary>
        /// <param name="metadata">Rented metadata, or <see langword="null" /> when nothing was rented.</param>
        internal static void Return(Metadata? metadata)
        {
            if (metadata == null)
                return;

            metadata.Clear();
            Pool.Add(metadata);
        }
    }

    /// <summary>Owns the optional client Activity and rented metadata for an outbound unary call.</summary>
    /// <typeparam name="TResponse">Outbound unary response type.</typeparam>
    [Immutable]
    private sealed class OutboundUnaryCallLease<TResponse>
    {
        private readonly AsyncUnaryCall<TResponse> _inner;
        private readonly Activity? _ownedActivity;
        private readonly Metadata? _rentedHeaders;
        private int _disposed;

        private OutboundUnaryCallLease(Activity? ownedActivity, Metadata? rentedHeaders, AsyncUnaryCall<TResponse> inner)
        {
            _ownedActivity = ownedActivity;
            _rentedHeaders = rentedHeaders;
            _inner = inner;
        }

        internal static AsyncUnaryCall<TResponse> WrapAsync(Activity? ownedActivity, Metadata? rentedHeaders, AsyncUnaryCall<TResponse> inner)
        {
            var lease = new OutboundUnaryCallLease<TResponse>(ownedActivity, rentedHeaders, inner);
            return new AsyncUnaryCall<TResponse>(lease.ResponseAsync(), inner.ResponseHeadersAsync, inner.GetStatus, inner.GetTrailers, lease.DisposeCall);
        }

        private void DisposeCall()
        {
            ReleaseActivity();
            _inner.Dispose();
        }

        private void ReleaseActivity()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _ownedActivity?.Dispose();
        }

        private async Task<TResponse> ResponseAsync()
        {
            try
            {
                // The owned client Activity and rented headers must live until the outbound unary call completes.
                // The rented bag is returned only from this completion path: an early outer dispose must not
                // recycle it while the transport may still read it.
                // ValueTask wraps the foreign gRPC task into one owned by this method (RemoteCache idiom).
                var responseAsync = _inner.ResponseAsync;
                return await new ValueTask<TResponse>(responseAsync).ConfigureAwait(false);
            }
            finally
            {
                ReleaseActivity();
                GrpcMetadataPool.Return(_rentedHeaders);
            }
        }
    }
}
