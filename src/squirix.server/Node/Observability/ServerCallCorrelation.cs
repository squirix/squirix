using System;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Opens the inbound trace activity, logging scope and deadline scope of a server call.</summary>
internal sealed class ServerCallCorrelation
{
    private readonly ILogger<ServerCallCorrelation> _log;
    private readonly string _nodeId;

    internal ServerCallCorrelation(ILogger<ServerCallCorrelation> log, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        _log = log;
        _nodeId = nodeId;
    }

    /// <summary>Starts the server activity, continuing the trace the caller sent when its headers are valid.</summary>
    /// <param name="context">The inbound call context.</param>
    /// <returns>The started activity, or <see langword="null" /> when nothing listens; dispose it when the call ends.</returns>
    internal static Activity? StartActivity(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var headers = context.RequestHeaders;
        var traceParent = headers.GetValue(Correlation.TraceParentHeader);
        var traceState = headers.GetValue(Correlation.TraceStateHeader);

        ActivityContext parent = default;
        if (!string.IsNullOrEmpty(traceParent))
            _ = ActivityContext.TryParse(traceParent, traceState, out parent);

        return ActivitySourceHolder.StartServer(context.Method, in parent);
    }

    /// <summary>Pushes the caller's deadline as the ambient budget of the call.</summary>
    /// <param name="context">The inbound call context.</param>
    /// <returns>The scope that restores the previous deadline when disposed.</returns>
    internal static IDisposable PushDeadline(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // gRPC turns the caller's timeout into an absolute deadline on the system clock, so that clock converts it back into a budget.
        return ServerRpcDeadlineContext.Push(context.Deadline, TimeProvider.System);
    }

    /// <summary>Opens the logging scope that carries the node id and the called method.</summary>
    /// <param name="context">The inbound call context.</param>
    /// <returns>The logging scope, or <see langword="null" /> when the logger has none; dispose it when the call ends.</returns>
    internal IDisposable? BeginLogScope(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return _log.BeginScope(Correlation.CreateScopeState(_nodeId, context.Method));
    }
}
