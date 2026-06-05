namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Provides storage diagnostics for admin REST endpoints.
/// </summary>
public interface IAdminStorageDiagnostics
{
    /// <summary>
    /// Gets the current storage diagnostics snapshot.
    /// </summary>
    /// <param name="recentSegmentLimit">Maximum number of recent journal segments to include.</param>
    /// <returns>Storage diagnostics snapshot.</returns>
    AdminStorageDiagnosticsSnapshot GetSnapshot(int recentSegmentLimit);
}
