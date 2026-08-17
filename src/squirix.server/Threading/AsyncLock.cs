using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Async mutual-exclusion lock backed by a single-slot <see cref="SemaphoreSlim"/>.</summary>
/// <remarks>Pair with <see cref="AsyncLockHolder"/> through <see langword="using"/> to release the lock on the scope exit.</remarks>
[Mutable]
internal sealed class AsyncLock : IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public void Dispose() => _semaphore.Dispose();

    internal async ValueTask<AsyncLockHolder> LockAsync(CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new AsyncLockHolder(_semaphore);
    }
}
