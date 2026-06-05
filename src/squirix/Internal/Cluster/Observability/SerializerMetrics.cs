using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>
/// Metrics for serialization operations.
/// </summary>
internal static class SerializerMetrics
{
    // Labels:
    //  - op: serialize|deserialize
    //  - exception_type: e.g., JsonException, ArgumentNullException
    //  - impl: serializer implementation
    public static readonly Counter3Labels FailuresTotal;

    // Labels:
    //  - op: serialize|deserialize
    //  - impl: serializer implementation
    public static readonly Histogram2Labels OpDurationSeconds;

    // Labels:
    //  - op: serialize|deserialize
    //  - result: ok|error
    //  - impl: serializer implementation (e.g., SystemTextJsonSerializer)
    public static readonly Counter3Labels OpsTotal;
    private static readonly Counter<long> FailuresTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_serializer_failures_total");
    private static readonly Histogram<double> OpDurationSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_serializer_op_duration_seconds");

    private static readonly Counter<long> OpsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_serializer_ops_total");

    static SerializerMetrics()
    {
        OpsTotal = new Counter3Labels(OpsTotalCtr, "op", "result", "impl");
        OpDurationSeconds = new Histogram2Labels(OpDurationSecondsHist, "op", "impl");
        FailuresTotal = new Counter3Labels(FailuresTotalCtr, "op", "exception_type", "impl");
    }
}
