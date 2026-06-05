using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>
/// Metrics for the client pool.
/// </summary>
internal static class ClientPoolMetrics
{
    private static readonly Counter<long> BootstrapWarmupSkippedTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_bootstrap_warmup_skipped_total");
    private static readonly Counter<long> DisposalsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_disposals_total");
    private static readonly Counter<long> WarmupsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_client_pool_warmups_total");
    private static int _gaugesRegistered;
    private static Func<int>? _observeDraining;
    private static Func<int>? _observePeerCount;

    private static Func<int>? _observePoolSize;

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

    /// <summary>
    /// Registers observable gauges backed by provided delegates.
    /// Gauges are created once; subsequent calls update the observation delegates only.
    /// </summary>
    /// <param name="observePoolSize">Delegate that returns the current number of clients in the pool.</param>
    /// <param name="observePeerCount">Delegate that returns the current number of configured peers.</param>
    /// <param name="observeDraining">Delegate that returns true if the pool is draining.</param>
    public static void RegisterObservers(Func<int> observePoolSize, Func<int> observePeerCount, Func<bool> observeDraining)
    {
        Volatile.Write(ref _observePoolSize, observePoolSize);
        Volatile.Write(ref _observePeerCount, observePeerCount);
        Volatile.Write(ref _observeDraining, () => observeDraining() ? 1 : 0);

        if (Interlocked.Exchange(ref _gaugesRegistered, 1) == 1)
            return;

        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_client_pool_size",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observePoolSize).Invoke()) },
            description: "Number of gRPC clients in the pool");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_client_peer_count",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observePeerCount).Invoke()) },
            description: "Number of peers configured in the pool");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_client_pool_draining",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observeDraining).Invoke()) },
            description: "Client pool is draining (1) or not (0)");
    }
}
