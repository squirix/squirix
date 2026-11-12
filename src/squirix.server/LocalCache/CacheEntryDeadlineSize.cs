namespace Squirix.Server.LocalCache;

/// <summary>Shared size estimate for the deadline index.</summary>
internal static class CacheEntryDeadlineSize
{
    /// <summary>Bytes charged for the deadline index when an entry has a deadline.</summary>
    /// <remarks>
    /// Two heap slots cover the live item and its share of stale items. Each slot holds a 24-byte item and an 8-byte priority.
    /// The heap rebuild bounds the heap at twice the entries plus 1024 items.
    /// An entry node stores its deadline in a field it already has.
    /// </remarks>
    internal const int IndexBytes = 2 * (24 + sizeof(long));
}
