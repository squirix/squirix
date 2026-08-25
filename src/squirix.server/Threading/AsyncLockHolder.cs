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

    public readonly override bool Equals([NotNullWhen(true)] object? obj) => obj is AsyncLockHolder other && Equals(other);

    public readonly override int GetHashCode() => HashCode.Combine(_semaphore, _released);

    public void Dispose()
    {
        if (_semaphore == null || Interlocked.Exchange(ref _released, 1) == 1)
            return;

        _ = _semaphore.Release();
    }

    public readonly bool Equals(AsyncLockHolder other) => _semaphore.Equals(other._semaphore) && _released == other._released;
}
