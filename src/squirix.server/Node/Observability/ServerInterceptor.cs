using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Server-side interceptor for inbound trace context and logging scope correlation.</summary>
internal sealed class ServerInterceptor : Interceptor
{
    private readonly ILogger<ServerInterceptor> _log;
    private readonly string _nodeId;

    internal ServerInterceptor(ILogger<ServerInterceptor> log, string nodeId)
    {
        _log = log;
        _nodeId = nodeId;
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var headers = context.RequestHeaders;
        var traceParent = headers.GetValue(Correlation.TraceParentHeader);
        var traceState = headers.GetValue(Correlation.TraceStateHeader);

        using var activity = StartServerActivity(traceParent, traceState, context.Method);
        using var scope = Correlation.BeginStandardScope(_log, _nodeId, context.Method);
        using var deadlineScope = ServerRpcDeadlineContext.Push(context.Deadline);
        return await base.UnaryServerHandler(request, context, continuation).ConfigureAwait(false);
    }

    private static Activity? StartServerActivity(string? traceParent, string? traceState, string method)
    {
        ActivityContext parent = default;
        if (!string.IsNullOrEmpty(traceParent))
            _ = ActivityContext.TryParse(traceParent, traceState, out parent);

        return ActivitySourceHolder.StartServer(method, in parent);
    }
}
