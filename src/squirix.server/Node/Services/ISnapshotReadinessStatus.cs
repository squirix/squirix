namespace Squirix.Server.Node.Services;

internal interface ISnapshotReadinessStatus
{
    bool HasFatalFailure { get; }
}
