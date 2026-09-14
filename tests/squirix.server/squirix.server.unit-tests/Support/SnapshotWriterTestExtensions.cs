using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test helpers for writing single-entry snapshots without one-element collection allocations.</summary>
internal static class SnapshotWriterTestExtensions
{
    private static readonly IReadOnlyList<PersistedIdempotencyRecord> NoIdempotencyRecords = [];

    internal static ValueTask<string> WriteSingleAsync(this ISnapshotWriter writer, int index, CacheKey key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
        writer.WriteAsync(index, new SingleItemReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>((key, entry)), NoIdempotencyRecords, cancellationToken);

    /// <summary>Zero-array <see cref="IReadOnlyList{T}" /> wrapper for a single test value.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    [Immutable]
    private sealed class SingleItemReadOnlyList<T> : IReadOnlyList<T>
    {
        private readonly T _item;

        internal SingleItemReadOnlyList(T item)
        {
            _item = item;
        }

        public int Count => 1;

        public T this[int index] => index == 0 ? _item : throw new ArgumentOutOfRangeException(nameof(index));

        public IEnumerator<T> GetEnumerator()
        {
            yield return _item;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
