namespace Squirix.Server.LocalCache;

/// <summary>O(1) observable statistics for the process-local physical cache store.</summary>
internal interface ILocalCacheStats
{
    /// <summary>Gets the number of entries currently held in the store.</summary>
    int EntryCount { get; }

    /// <summary>Gets the sum of the estimated bytes recorded for the entries currently held in the store.</summary>
    long EstimatedBytes { get; }
}
