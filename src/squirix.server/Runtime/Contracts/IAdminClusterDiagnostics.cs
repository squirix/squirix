namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Provides static cluster topology and ring diagnostics for admin REST endpoints.
/// </summary>
internal interface IAdminClusterDiagnostics
{
    /// <summary>
    /// Gets the current static cluster topology snapshot.
    /// </summary>
    /// <returns>Membership snapshot.</returns>
    AdminMembersDiagnosticsSnapshot GetMembersDiagnostics();

    /// <summary>
    /// Gets the core rebalance-history snapshot.
    /// </summary>
    /// <returns>An empty snapshot for core static routing.</returns>
    AdminRebalanceHistorySnapshot GetRebalanceHistory();

    /// <summary>
    /// Builds a ring distribution snapshot for the requested sample size.
    /// </summary>
    /// <param name="sampleSize">Number of synthetic keys to sample.</param>
    /// <returns>Ring diagnostics snapshot.</returns>
    AdminRingDiagnosticsSnapshot GetRingDiagnostics(int sampleSize);
}
