namespace Squirix.Server.Node.Observability;

/// <summary>Metrics for serialization operations.</summary>
internal static class ServerSerializerMetrics
{
    internal static readonly ServerCounter3Labels FailuresTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_serializer_failures_total"), "op", "exception_type", "impl");
    internal static readonly ServerHistogram2Labels OpDurationSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_serializer_op_duration_seconds"), "op", "impl");
    internal static readonly ServerCounter3Labels OpsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_serializer_ops_total"), "op", "result", "impl");
}
