namespace Squirix.Server.Storage.Journaling.Abstractions;

internal interface IJournalMetrics
{
    long AppendedBytes { get; }

    long AppendedOps { get; }

    double RecentAppendLatencyMs { get; }
}
