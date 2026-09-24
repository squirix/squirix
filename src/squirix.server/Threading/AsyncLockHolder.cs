using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Disposable handle returned by <see cref="AsyncLock.LockAsync" />; releases the lock when disposed.</summary>
/// <remarks>
/// Releasing never throws: after the owning lock was disposed while this holder was still out (a bounded shutdown
/// gave up waiting for it), the release only marks the lock free.
/// </remarks>
[ThreadSafe]
internal struct AsyncLockHolder : IDisposable, IEquatable<AsyncLockHolder>
{
    private readonly AsyncLock _owner;
    private int _released;

    internal AsyncLockHolder(AsyncLock owner)
    {
        _owner = owner;
    }

    public override readonly bool Equals([NotNullWhen(true)] object? obj) => obj is AsyncLockHolder other && Equals(other);

    public override readonly int GetHashCode() => HashCode.Combine(_owner, _released);

    public void Dispose()
    {
        if (_owner == null || Interlocked.Exchange(ref _released, 1) == 1)
            return;

        _owner.Release();
    }

    public readonly bool Equals(AsyncLockHolder other) => ReferenceEquals(_owner, other._owner) && _released == other._released;
}
