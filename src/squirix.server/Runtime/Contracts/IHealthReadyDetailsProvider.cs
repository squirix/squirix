namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Builds health-ready diagnostics for REST endpoints.
/// </summary>
internal interface IHealthReadyDetailsProvider
{
    /// <summary>
    /// Gets the current health-ready diagnostics snapshot.
    /// </summary>
    /// <returns>Health-ready diagnostics snapshot.</returns>
    HealthReadyDetailsSnapshot GetSnapshot();
}
