namespace Squirix.Server.LocalCache;

/// <summary>
/// Process-local physical cache for operations.
/// </summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCache<T> : ILocalCacheReadOperations<T>, ILocalCacheMutationOperations<T>, ILocalCacheRecovery<T>, ILocalCacheStats;
