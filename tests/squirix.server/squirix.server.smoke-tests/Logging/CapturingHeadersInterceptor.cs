using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Squirix.Server.SmokeTests.Logging;

/// <summary>
/// Test-only server-side gRPC interceptor that captures the latest request metadata headers.
/// Useful for asserting trace-context propagation in smoke tests.
/// </summary>
internal sealed class CapturingHeadersInterceptor : Interceptor
{
    private volatile Metadata? _last;

    /// <summary>
    /// Gets the last captured request metadata headers.
    /// </summary>
    public Metadata? LastRequestHeaders => _last;

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        _last = context.RequestHeaders;
        return await base.UnaryServerHandler(request, context, continuation);
    }
}
