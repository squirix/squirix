using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Disposable handle returned by <see cref="AsyncLock.LockAsync" />; releases the lock when disposed.</summary>
[ThreadSafe]
internal struct AsyncLockHolder : IDisposable, IEquatable<AsyncLockHolder>
{
    private readonly SemaphoreSlim _semaphore;
    private int _released;

    internal AsyncLockHolder(SemaphoreSlim semaphore)
    {
        _semaphore = semaphore;
    }

    public override readonly bool Equals([NotNullWhen(true)] object? obj) => obj is AsyncLockHolder other && Equals(other);

    public override readonly int GetHashCode() => HashCode.Combine(_semaphore, _released);

    public void Dispose()
    {
        if (_semaphore == null || Interlocked.Exchange(ref _released, 1) == 1)
            return;

        try
        {
            _ = _semaphore.Release();
        }
        catch (ObjectDisposedException)
        {
            // The owning lock was disposed while this holder was still out (a bounded shutdown gave up waiting for it):
            // nobody can wait on the semaphore anymore, so there is nothing left to release.
        }
    }

    public readonly bool Equals(AsyncLockHolder other) => _semaphore.Equals(other._semaphore) && _released == other._released;
}
