using System.Collections.Generic;
using Squirix.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.Storage.Snapshot;

[Immutable]
internal sealed record LoadResult<T>(List<(CacheKey Key, NodeCacheEntry<T> Entry)> Entries, IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecords);
