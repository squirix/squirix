namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// journal diagnostics for admin storage endpoints.
/// </summary>
public sealed class AdminJournalDiagnosticsSnapshot
{
    /// <summary>Gets the configured maximum number of recent segments.</summary>
    public required int RecentSegmentLimit { get; init; }

    /// <summary>Gets recent journal segment diagnostics.</summary>
    public required AdminJournalSegmentDiagnosticsSnapshot[] Segments { get; init; }
}
