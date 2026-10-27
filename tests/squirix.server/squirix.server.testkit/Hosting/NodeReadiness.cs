using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads what the readiness probe of a test node reports, the text of the failing check included.</summary>
/// <remarks>The ready endpoint answers with the status only; the text a failing check gives is read from the same checks the endpoint runs.</remarks>
internal static class NodeReadiness
{
    private const string ReplicaReadinessCheck = "replica_readiness";

    /// <summary>Runs the readiness checks of a node and reports the replica group check.</summary>
    /// <param name="host">A started in-process test node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status and the description of the replica group readiness check.</returns>
    /// <exception cref="InvalidOperationException">The node registers no replica group readiness check.</exception>
    internal static async Task<(HealthStatus Status, string Description)> ReplicaGroupsAsync(this ITestNodeHost host, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(host);
        var report = await host.GetRequiredService<HealthCheckService>()
                               .CheckHealthAsync(static registration => registration.Tags.Contains("ready"), cancellationToken)
                               .ConfigureAwait(false);
        return report.Entries.TryGetValue(ReplicaReadinessCheck, out var entry)
            ? (entry.Status, entry.Description ?? string.Empty)
            : throw new InvalidOperationException("The node registers no replica group readiness check.");
    }
}
