using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Builds health-ready diagnostics for REST endpoints.</summary>
internal interface IHealthReadyDetailsProvider
{
    /// <summary>Gets the current health-ready diagnostics snapshot.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Health-ready diagnostics snapshot.</returns>
    Task<HealthReadyDetailsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default);
}
