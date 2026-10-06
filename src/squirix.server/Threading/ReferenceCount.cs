using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>A thread-safe reference count that can be retained while it is positive and reports the release that brings it to zero.</summary>
/// <remarks>Once the count reached zero it stays there: <see cref="TryRetain" /> fails, so a resource freed by the last release is never handed out again.</remarks>
[ThreadSafe]
internal sealed class ReferenceCount
{
    private volatile int _count;

    /// <summary>Initializes a new instance of the <see cref="ReferenceCount" /> class.</summary>
    /// <param name="initialCount">The number of references held at the start; zero creates a count that is already released.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialCount" /> is negative.</exception>
    internal ReferenceCount(int initialCount = 1)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialCount);
        _count = initialCount;
    }

    /// <summary>Gets a value indicating whether the count reached zero.</summary>
    internal bool IsReleased => _count == 0;

    /// <summary>Drops one reference.</summary>
    /// <returns><see langword="true" /> when it was the last reference; this is reported to exactly one caller.</returns>
    /// <exception cref="InvalidOperationException">No reference is left to release.</exception>
    internal bool Release()
    {
        var count = _count;
        while (true)
        {
            if (count == 0)
                throw new InvalidOperationException("The reference count has no reference left to release.");

            var seen = Interlocked.CompareExchange(ref _count, count - 1, count);
            if (seen == count)
                return count == 1;

            count = seen;
        }
    }

    /// <summary>Adds one reference unless the count already reached zero.</summary>
    /// <returns><see langword="true" /> when the reference was added and must be released.</returns>
    internal bool TryRetain()
    {
        var count = _count;
        while (count > 0)
        {
            var seen = Interlocked.CompareExchange(ref _count, count + 1, count);
            if (seen == count)
                return true;

            count = seen;
        }

        return false;
    }
}
