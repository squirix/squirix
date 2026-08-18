using System;
using System.Threading;
using Squirix.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Disposable handle returned by <see cref="AsyncLock.LockAsync"/>; releases the lock when disposed.</summary>
[Mutable]
internal sealed class AsyncLockHolder : IDisposable
{
    private readonly SemaphoreSlim _semaphore;
    private int _released;

    internal AsyncLockHolder(SemaphoreSlim semaphore)
    {
        _semaphore = semaphore;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _released, 1) == 1)
            return;

        _ = _semaphore.Release();
    }
}
