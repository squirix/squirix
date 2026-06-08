using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>
/// Metrics for the client pool.
/// </summary>
internal static class ClientPoolMetrics
{
    private static readonly Counter<long> BootstrapWarmupSkippedTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_bootstrap_warmup_skipped_total");
    private static readonly Counter<long> DisposalsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_disposals_total");
    private static readonly Counter<long> WarmupsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_warmups_total");

    /// <summary>
    /// Records that a configured bootstrap peer was unreachable during warm-up while another peer succeeded.
    /// </summary>
    /// <param name="nodeId">Bootstrap peer node id.</param>
    /// <param name="reason">Failure classification (<c>connect_timeout</c> or <c>connect_failed</c>).</param>
    public static void AddBootstrapWarmupSkipped(string nodeId, string reason)
    {
        var tags = new TagList
        {
            { "node_id", nodeId },
            { "reason", reason },
        };
        BootstrapWarmupSkippedTotalCtr.Add(1, tags);
    }

    public static void AddDisposal() => DisposalsTotalCtr.Add(1);

    public static void AddWarmup() => WarmupsTotalCtr.Add(1);
}
