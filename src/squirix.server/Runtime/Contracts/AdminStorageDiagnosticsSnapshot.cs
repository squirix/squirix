namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Storage diagnostics snapshot for admin REST endpoints.
/// </summary>
public sealed class AdminStorageDiagnosticsSnapshot
{
    /// <summary>Gets the storage data directory.</summary>
    public required string DataDir { get; init; }

    /// <summary>Gets the current manifest snapshot.</summary>
    public required AdminManifestSnapshot Manifest { get; init; }

    /// <summary>Gets recent journal segment diagnostics.</summary>
    public required AdminJournalDiagnosticsSnapshot Journal { get; init; }

    /// <summary>Gets journal writer diagnostics.</summary>
    public required AdminJournalWriterDiagnosticsSnapshot Writer { get; init; }
}
