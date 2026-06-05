using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>
/// Trusted replay entry points used during durable recovery.
/// </summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal interface ILocalCacheRecovery<T>
{
    ValueTask InsertForDurableRecoveryAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken);

    ValueTask<bool> RemoveExpirationForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> RemoveForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken);

    ValueTask<bool> TouchExpirationForDurableRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken);
}
