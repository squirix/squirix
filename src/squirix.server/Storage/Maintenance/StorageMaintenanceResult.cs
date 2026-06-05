namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes the outcome of an offline storage maintenance action.
/// </summary>
internal sealed class StorageMaintenanceResult
{
    /// <summary>
    /// Gets the performed action name.
    /// </summary>
    public string Action { get; init; } = string.Empty;

    /// <summary>
    /// Gets the storage report captured after the action completed.
    /// </summary>
    public StorageMaintenanceReport Report { get; init; } = new();
}
