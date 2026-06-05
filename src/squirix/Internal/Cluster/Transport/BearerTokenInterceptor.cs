using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>
/// Resolves a bearer token before each gRPC call and attaches it as an <c>authorization</c> metadata header.
/// </summary>
internal sealed class BearerTokenInterceptor : Interceptor
{
    private const string HeaderName = "authorization";
    private readonly Func<CancellationToken, ValueTask<string>> _tokenProvider;

    public BearerTokenInterceptor(Func<CancellationToken, ValueTask<string>> tokenProvider)
    {
        _tokenProvider = tokenProvider ?? throw new ArgumentNullException(nameof(tokenProvider));
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(EnrichSync(context));

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(EnrichSync(context));

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(request, EnrichSync(context));

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation) => continuation(request, EnrichSync(context));

    private ClientInterceptorContext<TRequest, TResponse> EnrichSync<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var token = _tokenProvider(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        var headers = context.Options.Headers ?? [];
        headers.Add(HeaderName, $"Bearer {token}");
        var options = context.Options.WithHeaders(headers);
        return new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, options);
    }
}
