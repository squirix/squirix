namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Thread-safe approximate aggregate memory usage counters for cache admission heuristics (v0.7.x).
/// </summary>
internal interface IMemoryUsageAccounting
{
    /// <summary>
    /// Gets the running count of accounted live entries.
    /// </summary>
    long EntryCount { get; }

    /// <summary>
    /// Gets the running sum of estimated bytes for accounted live entries.
    /// </summary>
    long EstimatedBytes { get; }

    /// <summary>
    /// Gets the number of memory admission rejections recorded for this accounting scope.
    /// </summary>
    long RejectedWriteCount { get; }

    /// <summary>
    /// Applies one additional live entry with the given estimated size.
    /// </summary>
    /// <param name="estimatedBytes">Non-negative estimated footprint.</param>
    void AddEntry(long estimatedBytes);

    /// <summary>
    /// Records one memory admission rejection (diagnostics counter; paired with metrics in the gate).
    /// </summary>
    void RecordAdmissionRejection();

    /// <summary>
    /// Removes one live entry with the given estimated size.
    /// </summary>
    /// <param name="estimatedBytes">Non-negative estimated footprint.</param>
    void RemoveEntry(long estimatedBytes);

    /// <summary>
    /// Replaces one live entry: adjusts bytes by <paramref name="newEstimatedBytes" /> − <paramref name="oldEstimatedBytes" />.
    /// </summary>
    /// <param name="oldEstimatedBytes">Estimated footprint of the previous live entry.</param>
    /// <param name="newEstimatedBytes">Estimated footprint of the replacement live entry.</param>
    void ReplaceEntry(long oldEstimatedBytes, long newEstimatedBytes);
}
