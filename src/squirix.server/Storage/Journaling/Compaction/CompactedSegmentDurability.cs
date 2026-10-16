using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>
/// Flushes the compacted temp segment once, after its last frame. The segment is not opened write-through because
/// every frame is a separate write and per-frame write-through would turn compaction into one synchronous disk wait per entry.
/// </summary>
[Immutable]
internal sealed class CompactedSegmentDurability : ICompactedSegmentDurability
{
    /// <inheritdoc />
    public void Flush(SafeFileHandle handle) => RandomAccess.FlushToDisk(handle);
}
