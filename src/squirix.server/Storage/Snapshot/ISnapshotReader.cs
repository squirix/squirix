using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Loads snapshot files from durable storage.</summary>
internal interface ISnapshotReader
{
    /// <summary>Loads a snapshot file, failing on any corruption.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="path">The snapshot file path.</param>
    /// <param name="expiredAsOf">Entries whose deadline is at or before this server clock time are skipped; <see langword="null" /> keeps every entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The loaded entries and idempotency records.</returns>
    ValueTask<LoadResult<T>> LoadStrictAsync<T>(string path, DateTime? expiredAsOf, CancellationToken cancellationToken = default);
}
