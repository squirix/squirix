using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Merges the expiries of one key and feeds the distinct keys, in batches, to one runner.</summary>
/// <typeparam name="TEntry">The entry an expiry hands back when the key turns out to be live.</typeparam>
/// <remarks>
/// Concurrent expiries of one key share a single result. The keys wait in one queue; one runner takes up to <see cref="MaxBatchKeys" /> of
/// them at a time and hands them to the batch delegate, which commits their tombstones together. The shared result is never canceled by a
/// caller: past its local append a tombstone may commit, so a caller that stops waiting only leaves, and the batch goes on. A batch the
/// delegate refuses as a whole fails its keys and every key queued behind it with the same failure and ends the runner, so a refusing group
/// is not asked again in a loop; the next expiry starts a new runner. A key arriving at a full queue fails at once. Dispose refuses new
/// expiries, fails the keys not yet taken, and waits for the batch in flight for at most <see cref="ShutdownBudget" />; a batch still going
/// after it is reported through <see cref="ShutdownLeakReporter" /> and left to finish on its own instead of failing the dispose.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaExpirationCoordinator<TEntry> : IAsyncDisposable
    where TEntry : class
{
    private const int DefaultMaxBatchKeys = 64;
    private const int DefaultMaxQueuedKeys = 1024;

    private readonly QuiescenceGate _drain = new();
    private readonly Func<IReadOnlyList<(string CacheName, string Key)>, Task<(TEntry? Entry, Exception? Failure)[]>> _expireBatch;
    private readonly Lock _lifetimeSync = new();
    private readonly Queue<(string CacheName, string Key)> _queue = new();
    private readonly Dictionary<(string CacheName, string Key), TaskCompletionSource<TEntry?>> _waiting = [];
    private bool _accepting = true;
    private Task? _disposeTask;
    private bool _running;

    /// <summary>Initializes a new instance of the <see cref="ReplicaExpirationCoordinator{TEntry}" /> class.</summary>
    /// <param name="expireBatch">
    /// Expires distinct keys together: one result per key, in order, which is the live entry, <see langword="null" /> once the tombstone is
    /// committed or the key is absent, or the failure of that key. It throws when the whole batch is refused. It must end on its own, without a
    /// cancellation token, since one batch serves every caller of its keys.
    /// </param>
    internal ReplicaExpirationCoordinator(Func<IReadOnlyList<(string CacheName, string Key)>, Task<(TEntry? Entry, Exception? Failure)[]>> expireBatch)
    {
        ArgumentNullException.ThrowIfNull(expireBatch);
        _expireBatch = expireBatch;
        ShutdownBudget = TimeSpan.FromSeconds(30);
        MaxBatchKeys = DefaultMaxBatchKeys;
        MaxQueuedKeys = DefaultMaxQueuedKeys;
    }

    /// <summary>Initializes the most keys the queue holds that no runner took yet; 1024 unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The size is not positive.</exception>
    internal int MaxQueuedKeys
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);

            field = value;
        }
    }

    /// <summary>Initializes the most keys one batch holds; 64 unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The size is not positive.</exception>
    internal int MaxBatchKeys
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);

            field = value;
        }
    }

    /// <summary>Initializes the longest dispose wait for the batch in flight; 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            field = value;
        }
    }

    /// <summary>Initializes the owner callback that reports, with the shutdown budget, a dispose that left a batch running.</summary>
    /// <remarks>This namespace does not log; the owner turns the report into an error log. Unset, the leak is not reported.</remarks>
    internal Action<TimeSpan>? ShutdownLeakReporter { private get; init; }

    /// <summary>Initializes the time source of the shutdown budget.</summary>
    internal required TimeProvider ShutdownTimeProvider { private get; init; }

    /// <summary>Refuses new expiries, fails the keys not yet taken by the runner, and waits, within the shutdown budget, for the batch in flight.</summary>
    /// <returns>An asynchronous operation that never fails for a batch that outlasts the budget.</returns>
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource<TEntry?>[] dropped;
        ValueTask disposal;
        lock (_lifetimeSync)
        {
            _accepting = false;
            dropped = TakeQueuedLocked();
            _disposeTask ??= DisposeCoreAsync();
            disposal = new ValueTask(_disposeTask);
        }

        FailAll(dropped, new ObjectDisposedException(nameof(ReplicaExpirationCoordinator<>)));
        return disposal;
    }

    /// <summary>Expires a key, joining the expiry of that key already queued or in flight.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Ends this caller's wait only; the shared expiry goes on.</param>
    /// <returns><see langword="null" /> once the tombstone is committed or the key is absent; the live entry when the key is not expired.</returns>
    /// <exception cref="ObjectDisposedException">The coordinator is disposing or disposed.</exception>
    /// <remarks>A full queue fails the returned task with <see cref="InvalidOperationException" /> instead of waiting.</remarks>
    internal Task<TEntry?> ExpireAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        ExpireMany([(cacheName, key)], cancellationToken)[0];

    /// <summary>Expires several keys, queuing all of them before the runner takes any, so they go out in as few batches as the limits allow.</summary>
    /// <param name="keys">Target caches and keys; a key already queued or in flight is joined.</param>
    /// <param name="cancellationToken">Ends this caller's waits only; the shared expiries go on.</param>
    /// <returns>One task per key, in order, each as the task of <see cref="ExpireAsync" />.</returns>
    /// <exception cref="ObjectDisposedException">The coordinator is disposing or disposed.</exception>
    internal Task<TEntry?>[] ExpireMany(IReadOnlyList<(string CacheName, string Key)> keys, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var waits = new Task<TEntry?>[keys.Count];
        lock (_lifetimeSync)
        {
            ObjectDisposedException.ThrowIf(!_accepting, this);
            for (var i = 0; i < waits.Length; i++)
            {
                ArgumentException.ThrowIfNullOrEmpty(keys[i].CacheName);
                ArgumentException.ThrowIfNullOrEmpty(keys[i].Key);
                waits[i] = EnqueueLocked(keys[i]).Task.WaitAsync(cancellationToken);
            }

            if (!_running && _queue.Count > 0)
            {
                _running = true;
                _drain.Enter();

                // The runner starts on the pool; it takes the lock before it reads the queue, so it waits until every key is in it.
                StartRunner();
            }
        }

        return waits;
    }

    private static void FailAll(TaskCompletionSource<TEntry?>[] waiters, Exception failure)
    {
        foreach (var waiter in waiters)
            _ = waiter.TrySetException(failure);
    }

    /// <summary>Queues a key, or finds the queued or running expiry of it.</summary>
    /// <param name="key">The key.</param>
    /// <returns>The shared result of the key; already failed when the queue is full.</returns>
    /// <remarks>Must be called under <see cref="_lifetimeSync" />.</remarks>
    private TaskCompletionSource<TEntry?> EnqueueLocked((string CacheName, string Key) key)
    {
        if (_waiting.TryGetValue(key, out var shared))
            return shared;

        shared = new TaskCompletionSource<TEntry?>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (_queue.Count >= MaxQueuedKeys)
        {
            _ = shared.TrySetException(new InvalidOperationException("The expiry queue of the group is full."));
            return shared;
        }

        _waiting[key] = shared;
        _queue.Enqueue(key);
        return shared;
    }

    private async Task DisposeCoreAsync()
    {
        using var budget = new CancellationTokenSource(ShutdownBudget, ShutdownTimeProvider);
        try
        {
            await _drain.WaitAsync(budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            ShutdownLeakReporter?.Invoke(ShutdownBudget);
        }
    }

    /// <summary>Removes the keys still queued from the waiting set.</summary>
    /// <returns>The waiters of those keys.</returns>
    /// <remarks>Must be called under <see cref="_lifetimeSync" />.</remarks>
    private TaskCompletionSource<TEntry?>[] TakeQueuedLocked()
    {
        var taken = new TaskCompletionSource<TEntry?>[_queue.Count];
        for (var i = 0; i < taken.Length; i++)
        {
            var key = _queue.Dequeue();
            taken[i] = _waiting[key];
            _ = _waiting.Remove(key);
        }

        return taken;
    }

    /// <summary>Starts the runner on the pool, without the execution context of the caller that happens to start it.</summary>
    /// <remarks>
    /// The runner serves every caller, so it must not carry the ambient state of the first one, such as its operation scope. The flow
    /// suppression covers only the start, and is undone on this thread before the caller goes on.
    /// </remarks>
    private void StartRunner()
    {
        if (ExecutionContext.IsFlowSuppressed())
        {
            StartOnPool();
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                StartOnPool();
        }
    }

    private void StartOnPool() => _ = Task.Factory.StartNew(
        static state => state is ReplicaExpirationCoordinator<TEntry> coordinator
            ? coordinator.RunAsync()
            : throw new InvalidOperationException("The expiry runner was started without its state."),
        this,
        CancellationToken.None,
        TaskCreationOptions.DenyChildAttach,
        TaskScheduler.Default).Unwrap();

    private async Task RunAsync()
    {
        try
        {
            while (TakeBatch() is { } batch)
            {
                var answered = _expireBatch(batch.Keys);
                if (await answered.CaptureFailureAsync().ConfigureAwait(false) is { } refused)
                {
                    FailRefused(batch, refused);
                    return;
                }

                var results = await answered.ConfigureAwait(false);
                if (results.Length != batch.Keys.Length)
                {
                    FailRefused(batch, new InvalidOperationException("The expiry batch answered a different number of keys."));
                    return;
                }

                Complete(batch, results);
            }
        }
        finally
        {
            _drain.Exit();
        }
    }

    /// <summary>Takes the next keys off the queue, or ends the runner when there are none.</summary>
    /// <returns>The keys and their waiters; <see langword="null" /> when the runner ended.</returns>
    /// <remarks>The taken keys stay in the waiting set until their batch ends, so a later caller of one joins it.</remarks>
    private Batch? TakeBatch()
    {
        lock (_lifetimeSync)
        {
            if (_queue.Count == 0)
            {
                _running = false;
                return null;
            }

            var count = Math.Min(_queue.Count, MaxBatchKeys);
            var keys = new (string CacheName, string Key)[count];
            var waiters = new TaskCompletionSource<TEntry?>[count];
            for (var i = 0; i < count; i++)
            {
                keys[i] = _queue.Dequeue();
                waiters[i] = _waiting[keys[i]];
            }

            return new Batch(keys, waiters);
        }
    }

    private void Complete(Batch batch, (TEntry? Entry, Exception? Failure)[] results)
    {
        lock (_lifetimeSync)
        {
            foreach (var key in batch.Keys)
                _ = _waiting.Remove(key);
        }

        for (var i = 0; i < batch.Waiters.Length; i++)
            _ = results[i].Failure != null ? batch.Waiters[i].TrySetException(results[i].Failure!) : batch.Waiters[i].TrySetResult(results[i].Entry);
    }

    /// <summary>Fails a refused batch and every key queued behind it, and ends the runner.</summary>
    /// <param name="batch">The refused batch.</param>
    /// <param name="failure">The refusal.</param>
    private void FailRefused(Batch batch, Exception failure)
    {
        TaskCompletionSource<TEntry?>[] queued;
        lock (_lifetimeSync)
        {
            queued = TakeQueuedLocked();
            _running = false;
            foreach (var key in batch.Keys)
                _ = _waiting.Remove(key);
        }

        FailAll(batch.Waiters, failure);
        FailAll(queued, failure);
    }

    [Immutable]
    private sealed record Batch((string CacheName, string Key)[] Keys, TaskCompletionSource<TEntry?>[] Waiters);
}
