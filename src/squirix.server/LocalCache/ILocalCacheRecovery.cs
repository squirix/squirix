using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Trusted replay entry points used during durable recovery.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheRecovery<T>
{
    ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationRecoveryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> TouchExpirationRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken);
}
