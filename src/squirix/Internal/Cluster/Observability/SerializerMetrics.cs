namespace Squirix.Internal.Cluster.Observability;

/// <summary>Metrics for serialization operations.</summary>
internal static class SerializerMetrics
{
    internal static readonly Counter3Labels FailuresTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_serializer_failures_total"), "op", "exception_type", "impl");
    internal static readonly Histogram2Labels OpDurationSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_serializer_op_duration_seconds"), "op", "impl");
    internal static readonly Counter3Labels OpsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_serializer_ops_total"), "op", "result", "impl");
}
