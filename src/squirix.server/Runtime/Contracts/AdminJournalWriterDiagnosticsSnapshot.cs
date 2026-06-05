namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// journal writer diagnostics for admin storage endpoints.
/// </summary>
public readonly record struct AdminJournalWriterDiagnosticsSnapshot(int CurrentJournal, ulong NextSequence, long AppendedOps, long AppendedBytes, double RecentAppendLatencyMs);
