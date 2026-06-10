namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Privacy profile applied when exporting Prometheus text from the HTTP <c>/metrics</c> endpoint.
/// </summary>
internal enum PrometheusScrapeProfile
{
    /// <summary>
    /// Strip identifying labels and aggregate series before HTTP export.
    /// </summary>
    Public,
}
