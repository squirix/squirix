using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>Metrics for the client pool.</summary>
internal static class ClientPoolMetrics
{
    private static readonly Counter<long> BootstrapWarmupSkippedTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_bootstrap_warmup_skipped_total");
    private static readonly Counter<long> DisposeFailuresTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_dispose_failures_total");
    private static readonly Counter<long> DisposalsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_disposals_total");
    private static readonly Counter<long> WarmupsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_warmups_total");

    /// <summary>Records that a configured bootstrap peer was unreachable during warm-up while another peer succeeded.</summary>
    /// <param name="nodeId">Bootstrap peer node id.</param>
    /// <param name="reason">Failure classification (<c language="csharp">connect_timeout</c> or <c language="csharp">connect_failed</c>).</param>
    internal static void AddBootstrapWarmupSkipped(string nodeId, string reason)
    {
        var tags = new TagList
        {
            { "node_id", nodeId },
            { "reason", reason },
        };
        BootstrapWarmupSkippedTotalCtr.Add(1, in tags);
    }

    /// <summary>Records that releasing a peer's channel failed while the pool was disposed.</summary>
    /// <param name="nodeId">Bootstrap peer node id.</param>
    /// <param name="failure">The failure, recorded by its type name.</param>
    internal static void AddChannelDisposeFailure(string nodeId, Exception failure) => AddDisposeFailure(nodeId, "channel", failure);

    internal static void AddDisposal() => DisposalsTotalCtr.Add(1);

    /// <summary>Records that draining or disposing a peer's call policy failed while the pool was disposed.</summary>
    /// <param name="nodeId">Bootstrap peer node id.</param>
    /// <param name="failure">The failure, recorded by its type name.</param>
    internal static void AddPolicyDisposeFailure(string nodeId, Exception failure) => AddDisposeFailure(nodeId, "policy", failure);

    internal static void AddWarmup() => WarmupsTotalCtr.Add(1);

    private static void AddDisposeFailure(string nodeId, string stage, Exception failure)
    {
        var tags = new TagList
        {
            { "node_id", nodeId },
            { "stage", stage },
            { "exception_type", failure.GetType().Name },
        };
        DisposeFailuresTotalCtr.Add(1, in tags);
    }
}
