namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Thread-safe approximate aggregate memory usage counters for cache admission heuristics (v0.7.x).</summary>
internal interface IMemoryUsageAccounting
{
    /// <summary>Gets the number of entries the local cache holds, follower copies and recovered entries included.</summary>
    /// <returns>The current entry count.</returns>
    long ReadEntryCount();

    /// <summary>Gets the sum of the estimated bytes of the entries the local cache holds, follower copies and recovered entries included.</summary>
    /// <returns>The current estimated byte total.</returns>
    long ReadEstimatedBytes();

    /// <summary>Gets the number of memory admission rejections recorded for this accounting scope.</summary>
    /// <returns>The cumulative rejection count.</returns>
    long ReadRejectedWriteCount();

    /// <summary>Records one memory admission rejection (diagnostics counter; paired with metrics in the gate).</summary>
    void RecordAdmissionRejection();
}
