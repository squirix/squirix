using System;
using System.Diagnostics.Metrics;
using System.Threading;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Metrics for the server-side inter-node client pool.
/// </summary>
internal static class ClientPoolMetrics
{
    private static readonly Counter<long> DisposalsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_peer_pool_disposals_total");
    private static readonly Counter<long> WarmupsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_peer_pool_warmups_total");
    private static int _gaugesRegistered;
    private static Func<int>? _observeDraining;
    private static Func<int>? _observePeerCount;

    private static Func<int>? _observePoolSize;

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
            "squirix_peer_pool_size",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observePoolSize).Invoke()) },
            description: "Number of gRPC clients in the server peer pool");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_peer_pool_peer_count",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observePeerCount).Invoke()) },
            description: "Number of peers configured in the server peer pool");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_peer_pool_draining",
            static () => new[] { new Measurement<int>(Volatile.Read(ref _observeDraining).Invoke()) },
            description: "Server peer pool is draining (1) or not (0)");
    }
}
