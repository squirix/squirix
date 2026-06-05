using System.Collections.Generic;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Storage.Snapshot;

internal sealed record SnapshotLoadResult<T>(IReadOnlyList<(CacheKey Key, CacheEntry<T> Entry)> Entries, IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecords);
