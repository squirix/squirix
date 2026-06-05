using System;
using System.Collections.Generic;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>
/// Chains multiple <see cref="Interceptor" /> instances into a single interceptor
/// by applying them in order (first added = outermost).
/// </summary>
internal sealed class CompositeInterceptor : Interceptor
{
    private readonly IReadOnlyList<Interceptor> _interceptors;

    public CompositeInterceptor(IReadOnlyList<Interceptor> interceptors)
    {
        _interceptors = interceptors ?? throw new ArgumentNullException(nameof(interceptors));
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var chain = continuation;
        for (var i = _interceptors.Count - 1; i >= 0; i--)
        {
            var interceptor = _interceptors[i];
            var next = chain;
            chain = ctx => interceptor.AsyncClientStreamingCall(ctx, next);
        }

        return chain(context);
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var chain = continuation;
        for (var i = _interceptors.Count - 1; i >= 0; i--)
        {
            var interceptor = _interceptors[i];
            var next = chain;
            chain = ctx => interceptor.AsyncDuplexStreamingCall(ctx, next);
        }

        return chain(context);
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var chain = continuation;
        for (var i = _interceptors.Count - 1; i >= 0; i--)
        {
            var interceptor = _interceptors[i];
            var next = chain;
            chain = (req, ctx) => interceptor.AsyncServerStreamingCall(req, ctx, next);
        }

        return chain(request, context);
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var chain = continuation;
        for (var i = _interceptors.Count - 1; i >= 0; i--)
        {
            var interceptor = _interceptors[i];
            var next = chain;
            chain = (req, ctx) => interceptor.AsyncUnaryCall(req, ctx, next);
        }

        return chain(request, context);
    }
}
