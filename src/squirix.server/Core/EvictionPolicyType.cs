namespace Squirix.Server.Core;

/// <summary>
/// Specifies cache eviction strategies used when capacity limits are reached.
/// </summary>
internal enum EvictionPolicyType
{
    /// <summary>
    /// Least Recently Used — evicts entries that have not been accessed recently.
    /// </summary>
    Lru = 0,

    /// <summary>
    /// First In, First Out — evicts the oldest inserted entries first.
    /// </summary>
    Fifo = 1,

    /// <summary>
    /// Least Frequently Used — evicts entries with the lowest access count.
    /// </summary>
    Lfu = 2,
}
