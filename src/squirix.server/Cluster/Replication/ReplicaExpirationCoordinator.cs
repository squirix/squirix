using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Serializes leader-owned expiration and commits tombstones before exposing a miss.</summary>
[ThreadSafe]
internal sealed class ReplicaExpirationCoordinator : IAsyncDisposable
{
    private readonly ReplicaCommitCoordinator _commit;
    private readonly TaskCompletionSource<bool> _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ReplicaMutationGate _keyGate;
    private readonly bool _leaderAuthority;
    private readonly Lock _lifetimeSync = new();
    private bool _accepting = true;
    private int _activeOperations;
    private Task? _disposeTask;

    internal ReplicaExpirationCoordinator(ReplicaCommitCoordinator commit, bool leaderAuthority, int maxInFlight = 64)
    {
        ArgumentNullException.ThrowIfNull(commit);
        _commit = commit;
        _leaderAuthority = leaderAuthority;
        _keyGate = new ReplicaMutationGate(maxInFlight);
    }

    /// <summary>Stops admission and releases the key gate after active operations leave it.</summary>
    /// <returns>An asynchronous operation.</returns>
    public ValueTask DisposeAsync()
    {
        lock (_lifetimeSync)
        {
            _accepting = false;
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    internal async ValueTask<bool> CommitExpiredMissAsync(ReplicaExpirationRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.ReadRaw);
        ArgumentNullException.ThrowIfNull(request.PrepareTombstone);
        ArgumentException.ThrowIfNullOrEmpty(request.GroupId);
        ArgumentException.ThrowIfNullOrEmpty(request.CacheName);
        ArgumentException.ThrowIfNullOrEmpty(request.Key);
        if (request.UtcNow.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Expiration comparison requires a UTC timestamp.", nameof(request));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(request.Timeout, TimeSpan.Zero);
        using var operation = EnterOperation();
        if (!_leaderAuthority)
            return false;

        using var lease = await _keyGate.EnterAsync(HashCode.Combine(request.CacheName, request.Key), request.CancellationToken).ConfigureAwait(false);
        var candidate = await request.ReadRaw(request.CancellationToken).ConfigureAwait(false);
        if (candidate is { ExpiresUtc.Kind: not DateTimeKind.Utc })
            throw new ArgumentException("Expiration candidate requires a UTC timestamp.", nameof(request));
        if (candidate is not { } expired || expired.ExpiresUtc > request.UtcNow)
            return false;

        var operationId = ReplicaExpirationOperationId.Create(request.GroupId, request.CacheName, request.Key, expired.Version, expired.ExpiresUtc);
        var tombstone = request.PrepareTombstone(expired, operationId);
        _ = await _commit.CommitAsync(tombstone, request.Timeout, request.CancellationToken).ConfigureAwait(false);
        return true;
    }

    internal async ValueTask<T> SerializeTouchAsync<T>(string cacheName, string key, Func<CancellationToken, ValueTask<T>> touch, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(touch);
        using var operation = EnterOperation();
        using var lease = await _keyGate.EnterAsync(HashCode.Combine(cacheName, key), cancellationToken).ConfigureAwait(false);
        return await touch(cancellationToken).ConfigureAwait(false);
    }

    private async Task DisposeCoreAsync()
    {
        Task drained;
        lock (_lifetimeSync)
            drained = _activeOperations == 0 ? Task.CompletedTask : _drained.Task;

        await drained.ConfigureAwait(false);
        _keyGate.Dispose();
    }

    private OperationLease EnterOperation()
    {
        lock (_lifetimeSync)
        {
            ObjectDisposedException.ThrowIf(!_accepting, this);
            _activeOperations++;
            return new OperationLease(this);
        }
    }

    private void ExitOperation()
    {
        bool signal;
        lock (_lifetimeSync)
        {
            _activeOperations--;
            signal = !_accepting && _activeOperations == 0;
        }

        if (signal)
            _ = _drained.TrySetResult(true);
    }

    private sealed class OperationLease : IDisposable
    {
        private ReplicaExpirationCoordinator? _owner;

        internal OperationLease(ReplicaExpirationCoordinator owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            owner?.ExitOperation();
        }
    }
}
