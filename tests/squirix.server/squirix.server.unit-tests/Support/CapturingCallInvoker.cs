using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Unary-only peer client that records every request instance it is given and answers with a configured response or failure.</summary>
internal sealed class CapturingCallInvoker : IPeerClient
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

    public Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage
    {
        var name = Enum.GetName(operation) ?? string.Empty;
        Methods.Add(name);
        Requests.Add(request);
        var error = _failure?.Invoke();
        return CompleteAsync<T>(error, _respond?.Invoke(name));
    }

    private static Task<TResponse> CompleteAsync<TResponse>(Exception? failure, object? configured) => (failure, configured) switch
    {
        ({ } error, _) => Task.FromException<TResponse>(error),
        (_, TResponse response) => Task.FromResult(response),
        _ => Task.FromException<TResponse>(new InvalidOperationException("No response is configured for the call.")),
    };
}
