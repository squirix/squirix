namespace Squirix.Server.Storage.Journaling;

internal interface IJournalMetrics
{
    long AppendedBytes { get; }

    long AppendedOps { get; }

    double RecentAppendLatencyMs { get; }
}
