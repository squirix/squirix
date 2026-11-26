namespace Squirix.Server.LocalCache;

/// <summary>The result of a reader’s lookup before taking the cache lock.</summary>
internal enum CacheReadOutcome
{
    /// <summary>The key holds no entry.</summary>
    Absent = 0,

    /// <summary>The key holds an entry.</summary>
    Present = 1,

    /// <summary>A writer got in the way; read again under the cache lock.</summary>
    Unsure = 2,
}
