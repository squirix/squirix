using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Loads snapshot files from durable storage.</summary>
internal interface ISnapshotReader
{
    ValueTask<LoadResult<T>> LoadStrictAsync<T>(string path, bool skipExpired = true, CancellationToken cancellationToken = default);
}
