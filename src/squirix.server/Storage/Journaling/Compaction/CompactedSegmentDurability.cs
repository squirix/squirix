using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>
/// Flushes the compacted temp segment once, after its last frame, on every platform: the segment is not opened write-through,
/// unlike <see cref="Squirix.Server.Storage.Snapshot.Binary.SnapshotDurability" />, whose temp file is write-through and is also flushed.
/// The segment is not opened write-through because every frame is a separate write and per-frame write-through would turn
/// compaction into one synchronous disk wait per entry.
/// </summary>
[Immutable]
internal sealed class CompactedSegmentDurability : ICompactedSegmentDurability
{
    /// <inheritdoc />
    public void Flush(SafeFileHandle handle) => RandomAccess.FlushToDisk(handle);
}
