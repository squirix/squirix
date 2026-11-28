using System;
using System.Diagnostics;
using Squirix.Server.Attributes;
using Squirix.Server.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Opens the inbound trace activity and logging scope of a server call.</summary>
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

    internal IDisposable? BeginLogScope(string method) => _log.IsScopeEnabled ? _log.BeginScope(Correlation.CreateScopeState(_nodeId, method)) : null;
}
