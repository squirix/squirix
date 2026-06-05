using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Metrics for journal JSON codec (encode/decode of journal envelopes to/from JSON payloads).
/// </summary>
internal static class JournalJsonCodecMetrics
{
    private static readonly Histogram<double> OpDurationSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_journal_json_op_duration_seconds");
    private static readonly Counter<long> OpsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_journal_json_ops_total");
    private static readonly Counter<long> PayloadBytesTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_journal_json_payload_bytes_total");

    // Labels:
    //  - op: encode|decode
    //  - result: ok|error
    public static void AddOp(string op, string result)
    {
        var tags = new TagList
        {
            { "op", op },
            { "result", result },
        };
        OpsTotalCtr.Add(1, tags);
    }

    // Labels:
    //  - op: encode|decode
    public static void AddPayloadBytes(string op, long bytes)
    {
        var tags = new TagList
        {
            { "op", op },
        };
        PayloadBytesTotalCtr.Add(bytes, tags);
    }

    // Labels:
    //  - op: encode|decode
    public static void RecordDuration(string op, double seconds)
    {
        var tags = new TagList
        {
            { "op", op },
        };
        OpDurationSecondsHist.Record(seconds, tags);
    }
}
