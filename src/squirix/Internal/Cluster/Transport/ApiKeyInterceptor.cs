using System;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>
/// Attaches a static API key as the <c>x-api-key</c> metadata header on every gRPC call.
/// </summary>
internal sealed class ApiKeyInterceptor : Interceptor
{
    private const string HeaderName = "x-api-key";
    private readonly string _apiKey;

    public ApiKeyInterceptor(string apiKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(apiKey);
        _apiKey = apiKey;
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(Enrich(context));

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(Enrich(context));

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation) => continuation(request, Enrich(context));

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation) => continuation(request, Enrich(context));

    private ClientInterceptorContext<TRequest, TResponse> Enrich<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var headers = context.Options.Headers ?? [];
        headers.Add(HeaderName, _apiKey);
        var options = context.Options.WithHeaders(headers);
        return new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, options);
    }
}
