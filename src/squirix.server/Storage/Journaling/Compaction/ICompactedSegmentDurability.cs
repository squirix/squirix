using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Journaling.Compaction;

/// <summary>Makes a written compacted journal segment durable before it is published.</summary>
internal interface ICompactedSegmentDurability
{
    /// <summary>Flushes the written temp segment to stable storage before it is published.</summary>
    /// <param name="handle">The handle of the written temp segment.</param>
    void Flush(SafeFileHandle handle);
}
