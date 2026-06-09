namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Access control mode for the built-in Prometheus metrics endpoint.
/// </summary>
internal enum MetricsEndpointAccessMode
{
    /// <summary>
    /// Allow unauthenticated scrapes from any client.
    /// </summary>
    Anonymous,

    /// <summary>
    /// Allow unauthenticated scrapes from loopback clients only; all other clients must authenticate.
    /// </summary>
    LoopbackOrAuthenticated,
}
