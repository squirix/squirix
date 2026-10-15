using System;
using System.Collections.Generic;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Lists the stored keys past their deadline when committed records, not the local clock, decide expiry.</summary>
internal interface ILocalCacheExpiredKeys
{
    /// <summary>Lists the keys whose deadline passed and that no committed record removed yet, earliest deadline first.</summary>
    /// <param name="nowUtc">The instant the deadlines are compared with.</param>
    /// <param name="max">The most keys listed; a call examines at most four due keys per key asked for, so it may list fewer.</param>
    /// <param name="include">
    /// Picks the keys to list; the others are skipped but stay due, behind the keys due before this call. It runs under the cache lock, so it
    /// must be cheap and must not call into the cache.
    /// </param>
    /// <returns>The keys; none when the local clock decides expiry, since such a cache keeps no expired entry.</returns>
    IReadOnlyList<CacheKey> ListExpired(DateTime nowUtc, int max, Func<CacheKey, bool> include);
}
