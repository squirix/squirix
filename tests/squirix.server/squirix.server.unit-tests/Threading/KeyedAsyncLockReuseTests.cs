using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies bounded entry reuse preserves ownership, FIFO order and key isolation.</summary>
public sealed class KeyedAsyncLockReuseTests : ServerUnitTestBase
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    /// <summary>A copied old lease cannot release a reused entry or remove its new key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleLeaseCannotReleaseAnotherKey(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        var old = await locks.LockAsync(1, cancellationToken);
        var copy = old;
        old.Dispose();
        var current = await locks.LockAsync(2, cancellationToken);
        _ = await Assert.That(ReferenceEquals(copy.Entry, current.Entry)).IsTrue();
        _ = await Assert.That(copy.Holder.Ownership.Holds(current.Entry.Lock)).IsFalse();
        var queued = locks.LockAsync(2, cancellationToken);

        copy.Dispose();
        _ = await Assert.That(queued.IsCompleted).IsFalse();
        _ = await Assert.That(current.Holder.Ownership.Holds(current.Entry.Lock)).IsTrue();
        _ = await Assert.That(locks.Count).IsEqualTo(1);

        current.Dispose();
        (await queued.AsTask().WaitAsync(Guard, TimeProvider.System, cancellationToken)).Dispose();
        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    /// <summary>An idle entry can serve a new key without retaining the former key in the active table.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReusedEntryKeepsKeysIndependent(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        var previous = await locks.LockAsync(1, cancellationToken);
        previous.Dispose();
        using var current = await locks.LockAsync(2, cancellationToken);
        using var other = await locks.LockAsync(1, cancellationToken);

        _ = await Assert.That(ReferenceEquals(previous.Entry, current.Entry)).IsTrue();
        _ = await Assert.That(ReferenceEquals(current.Entry, other.Entry)).IsFalse();
        _ = await Assert.That(locks.Count).IsEqualTo(2);
    }

    /// <summary>A queued cancellation cannot return an entry still owned by the holder or its surviving waiter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledWaiterCannotPoolAnActiveEntry(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        var holder = await locks.LockAsync(1, cancellationToken);
        using var cancel = new CancellationTokenSource();
        var canceled = locks.LockAsync(1, cancel.Token);
        var survivor = locks.LockAsync(1, cancellationToken);
        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, KeyedAsyncLock<int>.Lease>(canceled);
        var independent = await locks.LockAsync(2, cancellationToken);
        _ = await Assert.That(ReferenceEquals(holder.Entry, independent.Entry)).IsFalse();
        independent.Dispose();

        holder.Dispose();
        var next = await survivor.AsTask().WaitAsync(Guard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(ReferenceEquals(holder.Entry, next.Entry)).IsTrue();
        _ = await Assert.That(locks.Count).IsEqualTo(1);
        next.Dispose();
        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    /// <summary>Pre-canceled acquisition returns its rented entry without granting ownership or losing it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreCanceledAcquireReturnsRentedEntry(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        var old = await locks.LockAsync(1, cancellationToken);
        old.Dispose();
        using var cancel = new CancellationTokenSource();
        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, KeyedAsyncLock<int>.Lease>(locks.LockAsync(2, cancel.Token));
        _ = await Assert.That(locks.Count).IsEqualTo(0);
        using var current = await locks.LockAsync(3, cancellationToken);
        _ = await Assert.That(ReferenceEquals(old.Entry, current.Entry)).IsTrue();
        old.Dispose();
        _ = await Assert.That(current.Holder.Ownership.Holds(current.Entry.Lock)).IsTrue();
    }

    /// <summary>A burst larger than the idle bound cannot retain an unbounded set of entry objects.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BurstRetainsOnlyBoundedIdleEntries(CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        var leases = new KeyedAsyncLock<int>.Lease[128];
        var entries = new HashSet<KeyedAsyncLock<int>.Entry>();
        for (var i = 0; i < leases.Length; i++)
        {
            leases[i] = await locks.LockAsync(i, cancellationToken);
            _ = entries.Add(leases[i].Entry);
        }

        for (var i = 0; i < leases.Length; i++)
            leases[i].Dispose();
        _ = await Assert.That(locks.Count).IsEqualTo(0);

        var reused = 0;
        for (var i = 0; i < leases.Length; i++)
        {
            leases[i] = await locks.LockAsync(i + leases.Length, cancellationToken);
            if (entries.Contains(leases[i].Entry))
                reused++;
        }

        _ = await Assert.That(reused).IsEqualTo(64);
        _ = await Assert.That(locks.Count).IsEqualTo(leases.Length);
        for (var i = 0; i < leases.Length; i++)
            leases[i].Dispose();
        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    /// <summary>Concurrent churn and copied releases never admit two owners of the same key.</summary>
    /// <param name="keys">The number of shared keys.</param>
    /// <param name="cancelQueued">Whether queued workers cancel alternating waits.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, false)]
    [Arguments(8, false)]
    [Arguments(1, true)]
    [Arguments(8, true)]
    public async Task ConcurrentReusePreservesExclusion(int keys, bool cancelQueued, CancellationToken cancellationToken)
    {
        var locks = new KeyedAsyncLock<int>();
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(Guard);
        var run = new ChurnRun(locks, keys, cancelQueued, guard.Token);
        var workers = new Task[16];
        for (var i = 0; i < workers.Length; i++)
            workers[i] = run.ExecuteAsync(i);
        run.Start();
        try
        {
            await Task.WhenAll(workers).WaitAsync(Guard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await guard.CancelAsync();
            await Task.WhenAll(workers).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        _ = await Assert.That(run.Violated).IsFalse();
        _ = await Assert.That(run.Completed > 0).IsTrue();
        if (!cancelQueued)
            _ = await Assert.That(run.Completed).IsEqualTo(workers.Length * 256);
        _ = await Assert.That(locks.Count).IsEqualTo(0);
    }

    private sealed class ChurnRun
    {
        private readonly int[] _active;
        private readonly bool _cancelQueued;
        private readonly KeyedAsyncLock<int> _locks;
        private readonly TaskCompletionSource _start = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationToken _token;
        private int _completed;
        private int _violated;

        internal ChurnRun(KeyedAsyncLock<int> locks, int keys, bool cancelQueued, CancellationToken token)
        {
            _locks = locks;
            _active = new int[keys];
            _cancelQueued = cancelQueued;
            _token = token;
        }

        internal int Completed => Volatile.Read(ref _completed);

        internal bool Violated => Volatile.Read(ref _violated) != 0;

        internal async Task ExecuteAsync(int worker)
        {
            await _start.Task.ConfigureAwait(false);
            KeyedAsyncLock<int>.Lease stale = default;
            for (var i = 0; i < 256; i++)
            {
                _token.ThrowIfCancellationRequested();
                var key = (worker + i) % _active.Length;
                using var cancel = CancellationTokenSource.CreateLinkedTokenSource(_token);
                var pending = _locks.LockAsync(key, cancel.Token);
                if (_cancelQueued && !pending.IsCompleted && (i & 1) == 0)
                    await cancel.CancelAsync().ConfigureAwait(false);

                KeyedAsyncLock<int>.Lease lease;
                try
                {
                    lease = await pending.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancel.IsCancellationRequested && !_token.IsCancellationRequested)
                {
                    continue;
                }

                stale.Dispose();
                if (Interlocked.Increment(ref _active[key]) != 1)
                    _ = Interlocked.Exchange(ref _violated, 1);
                try
                {
                    await Task.Yield();
                    _ = Interlocked.Increment(ref _completed);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _active[key]);
                    lease.Dispose();
                    stale = lease;
                }
            }
        }

        internal void Start() => _ = _start.TrySetResult();
    }
}
