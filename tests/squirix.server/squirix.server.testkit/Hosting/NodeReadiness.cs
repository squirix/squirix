using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Reads what the readiness probe of a test node reports, the text of the failing check included.</summary>
/// <remarks>The ready endpoint answers with the status only; the text a failing check gives is read from the same checks the endpoint runs.</remarks>
internal static class NodeReadiness
{
    private const string ReplicaReadinessCheck = "replica_readiness";

    internal static async ValueTask<bool> ReplicaGroupsReadyAsync(this ITestNodeHost host, CancellationToken cancellationToken) =>
        (await host.ReplicaGroupsAsync(cancellationToken).ConfigureAwait(false)).Status == ReadinessStatus.Healthy;

    /// <summary>Runs the readiness checks of a node and reports the replica group check.</summary>
    /// <param name="host">A started in-process test node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The status and the description of the replica group readiness check.</returns>
    /// <exception cref="InvalidOperationException">The node registers no replica group readiness check.</exception>
    internal static async Task<(ReadinessStatus Status, string Description)> ReplicaGroupsAsync(this ITestNodeHost host, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(host);
        var results = await host.GetRequiredService<ReadinessMonitor>().CheckAsync(cancellationToken).ConfigureAwait(false);
        foreach (var (name, result) in results)
        {
            if (string.Equals(name, ReplicaReadinessCheck, StringComparison.Ordinal))
                return (result.Status, result.Description);
        }

        throw new InvalidOperationException("The node registers no replica group readiness check.");
    }
}
