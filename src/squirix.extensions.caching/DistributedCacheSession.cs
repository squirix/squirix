using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Extensions.Caching;

/// <summary>Coordinates the single owned connection and the adapter's shutdown.</summary>
/// <typeparam name="T">The owned connection type.</typeparam>
internal sealed class DistributedCacheSession<T> : IAsyncDisposable
    where T : class, IAsyncDisposable
{
    private readonly TimeProvider _clock;
    private readonly Func<CancellationToken, Task<T>> _connect;
    private readonly Lock _gate = new();
    private readonly CancellationTokenSource _lifetime = new();
    private Task<T>? _connection;
    private Task? _disposal;
    private bool _stopping;

    internal DistributedCacheSession(Func<CancellationToken, Task<T>> connect, TimeProvider clock)
    {
        _connect = connect;
        _clock = clock;
        LifetimeToken = _lifetime.Token;
    }

    internal CancellationToken LifetimeToken { get; }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposal != null)
                return new ValueTask(_disposal);
            Volatile.Write(ref _stopping, true);
            _disposal = StopAsync(_connection);
            return new ValueTask(_disposal);
        }
    }

    internal Task<T> GetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var current = Volatile.Read(ref _connection);
        if (current is { IsCompletedSuccessfully: true })
        {
            ThrowIfDisposed();
            return current;
        }

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposal != null, this);
            if (_connection is { IsFaulted: false, IsCanceled: false })
                return _connection.WaitAsync(cancellationToken);
            var connecting = Task.Run(() => _connect(LifetimeToken), LifetimeToken);
            ObserveFault(connecting);
            Volatile.Write(ref _connection, connecting);
            return _connection.WaitAsync(cancellationToken);
        }
    }

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The connection task is known to have completed successfully.")]
    [SuppressMessage("Usage", "MA0045", Justification = "The completed task is read synchronously to avoid a second asynchronous operation.")]
    internal T? GetOpenedOrNull()
    {
        var current = Volatile.Read(ref _connection);
        return current is { IsCompletedSuccessfully: true } && !Volatile.Read(ref _stopping) ? current.GetAwaiter().GetResult() : null;
    }

    internal CancellationTokenSource? Link(CancellationToken caller)
    {
        caller.ThrowIfCancellationRequested();
        return caller.CanBeCanceled ? CancellationTokenSource.CreateLinkedTokenSource(caller, LifetimeToken) : null;
    }

    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _stopping), this);

    [SuppressMessage("Design", "CA1031", Justification = "Failed initialization has no session to release and its error belongs to its callers.")]
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The shared connection always starts on the thread pool and cannot require the disposal caller's context.")]
    private static async Task DisposeConnectedAsync(Task<T> connection)
    {
        T cache;
        try
        {
            cache = await connection.ConfigureAwait(false);
        }
        catch (Exception) when (connection.IsFaulted || connection.IsCanceled)
        {
            return;
        }

        await cache.DisposeAsync().ConfigureAwait(false);
    }

    private static void ObserveFault(Task task) => _ = task.ContinueWith(
        static completed => _ = completed.Exception,
        CancellationToken.None,
        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
        TaskScheduler.Default);

    [SuppressMessage("Usage", "VSTHRD003", Justification = "The shared connection starts on the thread pool and does not depend on the disposal caller's context.")]
    private async Task CleanupAsync(Task<T>? connection)
    {
        try
        {
            try
            {
                await _lifetime.CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                if (connection != null)
                    await DisposeConnectedAsync(connection).ConfigureAwait(false);
            }
        }
        finally
        {
            _lifetime.Dispose();
        }
    }

    private async Task StopAsync(Task<T>? connection)
    {
        var cleanup = CleanupAsync(connection);
        ObserveFault(cleanup);
        await cleanup.WaitAsync(TimeSpan.FromSeconds(30), _clock, CancellationToken.None).ConfigureAwait(false);
    }
}
