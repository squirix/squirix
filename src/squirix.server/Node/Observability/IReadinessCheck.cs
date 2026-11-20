using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.Observability;

internal interface IReadinessCheck
{
    /// <summary>Checks whether a node component is ready to serve requests.</summary>
    /// <param name="cancellationToken">The probe cancellation token.</param>
    /// <returns>The component status and its description.</returns>
    Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken);
}
