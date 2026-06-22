namespace Squirix.Server.Storage.Snapshot;

/// <summary>On-disk snapshot encoding backend.</summary>
public enum SnapshotBackend
{
    /// <summary>Legacy JSON frame snapshot files (<c>.ssqx</c>).</summary>
    Json = 0,

    /// <summary>Binary snapshot files (<c>.bsqx</c>).</summary>
    Binary = 1,
}
