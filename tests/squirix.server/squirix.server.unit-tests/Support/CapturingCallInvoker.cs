using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Unary-only <see cref="CallInvoker" /> that records every request instance it is given and answers with a configured response or failure.</summary>
internal sealed class CapturingCallInvoker : CallInvoker
{
    private readonly Func<Exception?>? _failure;
    private readonly Func<string, object?>? _respond;

    /// <summary>Initializes a new instance of the <see cref="CapturingCallInvoker" /> class.</summary>
    /// <param name="respond">Produces the response of a call from its method name; the calls return no response when omitted.</param>
    /// <param name="failure">Produces the failure every call ends with; the calls succeed when omitted or when it returns <see langword="null" />.</param>
    internal CapturingCallInvoker(Func<string, object?>? respond = null, Func<Exception?>? failure = null)
    {
        _respond = respond;
        _failure = failure;
    }

    /// <summary>Gets the names of the invoked methods, in call order.</summary>
    internal List<string> Methods { get; } = [];

    /// <summary>Gets the request instances, in call order.</summary>
    internal List<object> Requests { get; } = [];

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) => throw new NotSupportedException("The capturing invoker supports unary calls only.");

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options) => throw new NotSupportedException("The capturing invoker supports unary calls only.");

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request) => throw new NotSupportedException("The capturing invoker supports unary calls only.");

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        Methods.Add(method.Name);
        Requests.Add(request);
        return new AsyncUnaryCall<TResponse>(
            CompleteAsync<TResponse>(_failure?.Invoke(), _respond?.Invoke(method.Name)),
            Task.FromResult(new Metadata()),
            static () => new Status(StatusCode.OK, string.Empty),
            static () => [],
            static () => { });
    }

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException("The capturing invoker supports asynchronous calls only.");

    private static Task<TResponse> CompleteAsync<TResponse>(Exception? failure, object? configured) => (failure, configured) switch
    {
        ({ } error, _) => Task.FromException<TResponse>(error),
        (_, TResponse response) => Task.FromResult(response),
        _ => Task.FromException<TResponse>(new InvalidOperationException("No response is configured for the call.")),
    };
}
