using System.Collections.Generic;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Snapshot;

internal sealed record LoadResult<T>(List<(CacheKey Key, NodeCacheEntry<T> Entry)> Entries, IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecords);
