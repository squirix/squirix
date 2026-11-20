namespace Squirix.Server.Node.Observability;

internal enum ReadinessStatus
{
    /// <summary>The node is not ready to serve requests.</summary>
    Unhealthy = 0,

    /// <summary>The node is ready with reduced health.</summary>
    Degraded = 1,

    /// <summary>The node is ready.</summary>
    Healthy = 2,
}
