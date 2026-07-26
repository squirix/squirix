using System;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Active trace scope for snapshot creation.</summary>
internal interface ISnapshotTraceScope : IDisposable
{
    /// <summary>Sets a tag on the active snapshot trace span.</summary>
    /// <param name="key">Tag name.</param>
    /// <param name="value">Tag value (string to avoid boxing value types).</param>
    void SetTag(string key, string? value);
}
