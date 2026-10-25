namespace Squirix.Server.Node.Services;

internal interface ISnapshotReadinessStatus
{
    /// <summary>Gets the last snapshot failure while snapshots keep failing, or <see langword="null" /> when the last attempt succeeded.</summary>
    SnapshotFailure? CurrentFailure { get; }

    /// <summary>Gets a value indicating whether the snapshot trigger stopped for good and needs a restart.</summary>
    bool HasFatalFailure { get; }
}
