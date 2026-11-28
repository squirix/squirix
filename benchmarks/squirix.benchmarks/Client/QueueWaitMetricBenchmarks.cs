using System;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using Squirix.Internal.Cluster.Observability;

namespace Squirix.Benchmarks.Client;

/// <summary>Measures queue-wait timing and tag recording with a subscriber.</summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class QueueWaitMetricBenchmarks
{
    private const int Batch = 16_384;
    private MeterListener? _listener;

    /// <summary>Subscribes to the queue-wait histogram on the SDK meter.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _listener = new MeterListener
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, MeterRegistry.Meter) && string.Equals(instrument.Name, "squirix_call_policy_queue_wait_seconds", StringComparison.Ordinal))
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        _listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        _listener.Start();
    }

    /// <summary>Records queue-wait samples without acquiring a peer permit.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void QueueWaitMetricObserveBatched()
    {
        for (var i = 0; i < Batch; i++)
            CallPolicyMetrics.FinishQueueWait("node-a", CallPolicyMetrics.StartQueueWait());
    }

    /// <summary>Detaches the histogram listener.</summary>
    [GlobalCleanup]
    public void Cleanup() => _listener?.Dispose();
}
