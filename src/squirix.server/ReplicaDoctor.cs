using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server;

/// <summary>Builds offline replica diagnostics without starting a node.</summary>
public static class ReplicaDoctor
{
    /// <summary>Builds an offline replica diagnostics report from durable state only.</summary>
    /// <param name="options">The validated server options describing the configured topology.</param>
    /// <param name="dataDirectory">The node data directory holding the stamp and group metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The diagnostic report.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="options" /> fails validation.</exception>
    public static Task<ReplicaDoctorReport> BuildReportAsync(SquirixServerOptions options, string dataDirectory, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(dataDirectory);
        return BuildReportCoreAsync(options, dataDirectory, cancellationToken);
    }

    private static async Task<ReplicaDoctorReport> BuildReportCoreAsync(SquirixServerOptions options, string dataDirectory, CancellationToken cancellationToken)
    {
        var topology = Configurator.ToClusterConfig(options);
        var expected = TopologyFingerprint.CreateFromTopology(topology, MtlsOptionsResolver.ResolveFromEnvironment());
        var groupIds = new string[topology.Peers.Length];
        for (var i = 0; i < groupIds.Length; i++)
            groupIds[i] = topology.Peers[i].NodeId;

        var (hasMismatch, lines) = await ReplicaDoctorReportBuilder.BuildAsync(
            expected.ToString(),
            topology.ConfigurationGeneration,
            topology.ReplicaCount,
            groupIds,
            dataDirectory,
            cancellationToken).ConfigureAwait(false);
        return new ReplicaDoctorReport(hasMismatch, lines);
    }
}
