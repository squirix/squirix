using System;
using System.Diagnostics;
using Squirix.Server.Attributes;
using Squirix.Server.Logging;
using Squirix.Transport.Binary;

namespace Squirix.Server.Node.Observability;

/// <summary>Correlates inbound RPC result logs with their trace activity.</summary>
[Immutable]
internal sealed class ServerCallCorrelation
{
    private readonly IServerLogger<ServerCallCorrelation> _log;
    private readonly string _nodeId;

    internal ServerCallCorrelation(IServerLogger<ServerCallCorrelation> log, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        _log = log;
        _nodeId = nodeId;
    }

    internal static Activity? StartActivity(string method, string? traceParent, string? traceState)
    {
        if (!ActivitySourceHolder.HasListeners)
            return null;

        ActivityContext parent = default;
        if (!string.IsNullOrEmpty(traceParent))
            _ = ActivityContext.TryParse(traceParent, traceState, out parent);

        return ActivitySourceHolder.StartServer(method, in parent);
    }

    internal object? CreateLogScope(string method, Activity? activity) =>
        _log.IsScopeEnabled ? Correlation.CreateScopeState(activity, _nodeId, method) : null;

    internal void LogResult(object? scope, FrameStatus status)
    {
        if (scope == null || !_log.IsEnabled(ServerLogLevel.Debug))
            return;

        _log.LogWithScope(ServerLogLevel.Debug, new ServerLogEventId(3099, "ServerRpcResult"), status, null, static (result, _) => $"Server RPC completed with {result}.", scope);
    }
}
