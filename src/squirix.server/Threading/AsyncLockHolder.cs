using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Disposable handle returned by <see cref="AsyncLock.LockAsync" />; releases the lock when disposed.</summary>
/// <remarks>
/// Releasing never throws: after the owning lock was disposed while this holder was still out (a bounded shutdown
/// gave up waiting for it), the release only marks the lock free. The release names this acquisition's generation, so
/// a copy of a holder that was already released cannot release the acquisition that came after it.
/// </remarks>
[ThreadSafe]
internal struct AsyncLockHolder : IDisposable, IEquatable<AsyncLockHolder>
{
    private readonly ulong _generation;
    private readonly AsyncLock _owner;
    private int _released;

    internal AsyncLockHolder(AsyncLock owner, ulong generation)
    {
        _owner = owner;
        _generation = generation;
    }

    /// <summary>Gets the owner capability of this acquisition, to pass to the surfaces the lock protects.</summary>
    /// <remarks>It holds the lock until this holder releases; a <see langword="default" /> holder yields an ownership that never holds.</remarks>
    internal readonly AsyncLockOwnership Ownership => new(_owner, _generation);

    public override readonly bool Equals([NotNullWhen(true)] object? obj) => obj is AsyncLockHolder other && Equals(other);

    public override readonly int GetHashCode() => HashCode.Combine(_owner, _generation, _released);

    public void Dispose()
    {
        if (_owner == null || Interlocked.Exchange(ref _released, 1) == 1)
            return;

        _owner.Release(_generation);
    }

    public readonly bool Equals(AsyncLockHolder other) => ReferenceEquals(_owner, other._owner) && _generation == other._generation && _released == other._released;
}
