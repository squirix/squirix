using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Applies replica writes whose durability is tracked outside the cache journal.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface IReplicaCacheMutations<T>
{
    ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, bool replicaApply, CancellationToken cancellationToken, object? scope = null);

    ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, int? encodedLength, (bool ReplicaApply, object? Scope) mutation, CancellationToken cancellationToken);
}
