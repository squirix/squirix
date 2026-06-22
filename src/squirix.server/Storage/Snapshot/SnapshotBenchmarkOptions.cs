namespace Squirix.Server.Storage.Snapshot;

/// <summary>Benchmark parameter helpers for snapshot backend selection.</summary>
public static class SnapshotBenchmarkOptions
{
    /// <summary>Maps benchmark parameter values to <see cref="SnapshotBackend" />.</summary>
    /// <param name="value">0 = JSON, 1 = binary.</param>
    /// <returns>The resolved snapshot backend.</returns>
    public static SnapshotBackend BackendFromValue(int value) =>
        value switch
        {
            0 => SnapshotBackend.Json,
            1 => SnapshotBackend.Binary,
            _ => SnapshotBackend.Json,
        };
}
