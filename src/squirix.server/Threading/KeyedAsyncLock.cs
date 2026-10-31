using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Async mutual-exclusion lock per key: acquisitions of one key are granted in FIFO order, acquisitions of different keys never wait for each other.</summary>
/// <typeparam name="TKey">Key type; compared with its default equality.</typeparam>
/// <remarks>
/// Each active key owns one <see cref="AsyncLock" />. Its entry leaves the table when the last user (holder or queued waiter) leaves, so
/// the table holds only keys that are held or awaited. A bounded pool supplies entries without retaining idle keys.
/// Reuse keeps the lock's ownership generations, so a stale lease cannot release another key's acquisition.
/// A queued acquisition is cancellable and leaves the queue at once.
/// </remarks>
[ThreadSafe]
internal sealed class KeyedAsyncLock<TKey>
    where TKey : notnull
{
    private const int MaxPooledEntries = 64;

    private readonly Dictionary<TKey, Entry> _entries = [];
    private readonly Stack<Entry> _idleEntries = new(MaxPooledEntries);
    private readonly Lock _sync = new();

    /// <summary>Gets the number of keys currently held or awaited.</summary>
    internal int Count
    {
        get
        {
            lock (_sync)
                return _entries.Count;
        }
    }

    /// <summary>Acquires the lock of <paramref name="key" />, waiting in FIFO order while another acquisition of the same key holds it.</summary>
    /// <param name="key">Key to lock.</param>
    /// <param name="cancellationToken">Cancels the wait while the acquisition is still queued.</param>
    /// <returns>The lease that releases the key when disposed.</returns>
    internal ValueTask<Lease> LockAsync(TKey key, CancellationToken cancellationToken)
    {
        Entry entry;
        ValueTask<AsyncLockHolder> pending;

        // The wait is queued under the table lock, so the per-key queue order is the call order.
        lock (_sync)
        {
            if (!_entries.TryGetValue(key, out var existing))
            {
                existing = _idleEntries.TryPop(out var idle) ? idle : new Entry();
                _entries.Add(key, existing);
            }

            entry = existing;
            entry.Users++;
            pending = entry.Lock.LockAsync(cancellationToken);
        }

        return AwaitQueuedAsync(key, entry, pending);
    }

    private async ValueTask<Lease> AwaitQueuedAsync(TKey key, Entry entry, ValueTask<AsyncLockHolder> pending)
    {
        try
        {
            var holder = await pending.ConfigureAwait(false);
            return new Lease(this, key, entry, holder);
        }
        catch
        {
            Leave(key, entry);
            throw;
        }
    }

    private void Leave(TKey key, Entry entry)
    {
        lock (_sync)
        {
            entry.Users--;
            if (entry.Users == 0)
            {
                _ = _entries.Remove(key);

                // Every holder and canceled waiter has left before reuse; the lock generation is never reset.
                if (_idleEntries.Count < MaxPooledEntries)
                    _idleEntries.Push(entry);
            }
        }
    }

    /// <summary>Releases the key lock when disposed; disposing any copy again, or a default lease, does nothing.</summary>
    /// <param name="Owner">Table the key belongs to.</param>
    /// <param name="Key">Locked key.</param>
    /// <param name="Entry">Per-key lock entry.</param>
    /// <param name="Holder">Holder of the per-key lock.</param>
    [Immutable]
    internal readonly record struct Lease(KeyedAsyncLock<TKey> Owner, TKey Key, Entry Entry, AsyncLockHolder Holder) : IDisposable
    {
        /// <inheritdoc />
        public void Dispose()
        {
            // The holder's release is generation-checked, so only the first dispose of any copy leaves the entry.
            if (Owner != null && Holder.TryRelease())
                Owner.Leave(Key, Entry);
        }
    }

    /// <summary>Per-key lock and the number of acquisitions (held or queued) that reference it; guarded by the table lock.</summary>
    internal sealed class Entry
    {
        internal AsyncLock Lock { get; } = new();

        internal int Users { get; set; }
    }
}
