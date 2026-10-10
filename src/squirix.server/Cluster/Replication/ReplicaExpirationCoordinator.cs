using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Runs one leader expiry per key at a time and bounds how long dispose waits for the expiries in flight.</summary>
/// <typeparam name="TEntry">The entry an expiry hands back when the key turns out to be live.</typeparam>
/// <remarks>
/// Concurrent expiries of one key share a single run of the expiry delegate, which commits the tombstone or finds the key live. The shared
/// run is never canceled by a caller: past its local append a tombstone may commit, so a caller that stops waiting only leaves, and the run
/// goes on. Dispose refuses new expiries and waits for the runs in flight for at most <see cref="ShutdownBudget" />; a run still going after
/// it is reported through <see cref="ShutdownLeakReporter" /> and left to finish on its own instead of failing the dispose.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaExpirationCoordinator<TEntry> : IAsyncDisposable
    where TEntry : class
{
    private readonly QuiescenceGate _drain = new();
    private readonly Func<string, string, Task<TEntry?>> _expire;
    private readonly Dictionary<(string CacheName, string Key), Task<TEntry?>> _inFlight = [];
    private readonly Lock _lifetimeSync = new();
    private bool _accepting = true;
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="ReplicaExpirationCoordinator{TEntry}" /> class.</summary>
    /// <param name="expire">
    /// Expires one key: commits its tombstone and returns <see langword="null" />, or returns the live entry when the key is not expired. It
    /// must end on its own, without a cancellation token, since one run serves every caller of the key.
    /// </param>
    internal ReplicaExpirationCoordinator(Func<string, string, Task<TEntry?>> expire)
    {
        ArgumentNullException.ThrowIfNull(expire);
        _expire = expire;
        ShutdownBudget = TimeSpan.FromSeconds(30);
    }

    /// <summary>Initializes the longest dispose wait for the expiries in flight; 30 seconds unless set.</summary>
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

    /// <summary>Initializes the owner callback that reports, with the shutdown budget, a dispose that left an expiry running.</summary>
    /// <remarks>This namespace does not log; the owner turns the report into an error log. Unset, the leak is not reported.</remarks>
    internal Action<TimeSpan>? ShutdownLeakReporter { private get; init; }

    /// <summary>Initializes the time source of the shutdown budget; the system clock unless set.</summary>
    /// <remarks>Test seam: production coordinators keep the system clock.</remarks>
    internal TimeProvider ShutdownTimeProvider { private get; init; } = TimeProvider.System;

    /// <summary>Refuses new expiries and waits, within the shutdown budget, for the expiries in flight.</summary>
    /// <returns>An asynchronous operation that never fails for an expiry that outlasts the budget.</returns>
    public ValueTask DisposeAsync()
    {
        lock (_lifetimeSync)
        {
            _accepting = false;
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    /// <summary>Expires a key, joining the expiry of that key already in flight.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="cancellationToken">Ends this caller's wait only; the shared expiry goes on.</param>
    /// <returns><see langword="null" /> once the tombstone is committed or the key is absent; the live entry when the key is not expired.</returns>
    /// <exception cref="ObjectDisposedException">The coordinator is disposing or disposed.</exception>
    internal Task<TEntry?> ExpireAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(cacheName);
        ArgumentException.ThrowIfNullOrEmpty(key);
        Task<TEntry?> shared;
        lock (_lifetimeSync)
        {
            ObjectDisposedException.ThrowIf(!_accepting, this);
            if (!_inFlight.TryGetValue((cacheName, key), out shared!))
            {
                _drain.Enter();

                // The run starts on the pool; its completion takes the lock to leave the in-flight set, so it waits until the run is in it.
                shared = StartRunAsync(cacheName, key);
                _inFlight[(cacheName, key)] = shared;
            }
        }

        return shared.WaitAsync(cancellationToken);
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

    /// <summary>Starts the run of an expiry on the pool, without the execution context of the caller that happens to start it.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <returns>The run.</returns>
    /// <remarks>
    /// The run serves every caller of the key, so it must not carry the ambient state of the first one, such as its operation scope. The flow
    /// suppression covers only the start, and is undone on this thread before the caller goes on.
    /// </remarks>
    private Task<TEntry?> StartRunAsync(string cacheName, string key)
    {
        Task<TEntry?> run;
        if (ExecutionContext.IsFlowSuppressed())
        {
            run = StartOnPoolAsync(cacheName, key);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                run = StartOnPoolAsync(cacheName, key);
        }

        return run;
    }

    private Task<TEntry?> StartOnPoolAsync(string cacheName, string key) => Task.Factory.StartNew(
        static state => state is PoolRun run
            ? run.Coordinator.RunAsync(run.CacheName, run.Key)
            : throw new InvalidOperationException("The expiry run was started without its state."),
        new PoolRun(this, cacheName, key),
        CancellationToken.None,
        TaskCreationOptions.DenyChildAttach,
        TaskScheduler.Default).Unwrap();

    private async Task<TEntry?> RunAsync(string cacheName, string key)
    {
        try
        {
            return await _expire(cacheName, key).ConfigureAwait(false);
        }
        finally
        {
            lock (_lifetimeSync)
                _ = _inFlight.Remove((cacheName, key));

            _drain.Exit();
        }
    }

    private sealed record PoolRun(ReplicaExpirationCoordinator<TEntry> Coordinator, string CacheName, string Key);
}
