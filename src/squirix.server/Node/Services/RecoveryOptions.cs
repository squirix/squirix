namespace Squirix.Server.Node.Services;

/// <summary>
/// Controls whether RecoveryService blocks host startup until recovery completes.
/// </summary>
internal sealed class RecoveryOptions
{
    /// <summary>
    /// Gets a value indicating whether if true (default), RecoveryService.StartAsync awaits the recovery work.
    /// If false, recovery runs in the background and StartAsync returns immediately.
    /// </summary>
    public bool BlockOnStart { get; init; } = true;
}
