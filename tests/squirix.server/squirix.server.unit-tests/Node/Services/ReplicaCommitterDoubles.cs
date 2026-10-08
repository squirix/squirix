using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Doubles shared by the replica group committer tests: a two-node group, an accepting follower, and a scripted local cache.</summary>
internal static class ReplicaCommitterDoubles
{
    /// <summary>How the scripted local cache applies a replicated write.</summary>
    internal enum ApplyMode
    {
        /// <summary>The apply fails until the cache recovers, and then succeeds.</summary>
        Fail = 0,

        /// <summary>The apply stalls, ignoring cancellation, until released.</summary>
        Stall = 1,

        /// <summary>The first apply stalls, ignoring cancellation, until released and then fails; every later apply fails at once.</summary>
        StallThenFail = 2,
    }

    [Immutable]
    internal sealed class TwoNodeLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 2;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            destination[1] = "n2";
        }
    }

    /// <summary>Follower double that always holds the leader batch; an optional callback runs on each append, after the local append.</summary>
    [Immutable]
    internal sealed class AcceptingGateway : IReplicaRpcGateway
    {
        private readonly Action? _onAppend;

        internal AcceptingGateway(Action? onAppend = null)
        {
            _onAppend = onAppend;
        }

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            _onAppend?.Invoke();
            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last));
        }
    }

    /// <summary>Follower double that always holds the leader batch; once armed, the next call parks, ignoring cancellation, until released.</summary>
    [ThreadSafe]
    internal sealed class ParkingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _heldReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        /// <summary>Gets the node whose appends that carry entries park, ignoring cancellation, until <see cref="ReleaseHeld" />; none unless set.</summary>
        internal string? HeldNode { get; init; }

        /// <summary>Gets a task that completes when a call parked.</summary>
        internal Task Entered => _entered.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (string.Equals(nodeId, HeldNode, StringComparison.Ordinal) && batch.Records.Count > 0)
                await new ValueTask(_heldReleased.Task).ConfigureAwait(false);

            if (Interlocked.Exchange(ref _armed, 0) != 0)
            {
                _ = _entered.TrySetResult();
                await new ValueTask(_released.Task).ConfigureAwait(false);
            }

            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last);
        }

        /// <summary>Lets the parked appends of the held node, and every later one, answer.</summary>
        internal void ReleaseHeld() => _ = _heldReleased.TrySetResult();

        /// <summary>Makes the next call park.</summary>
        internal void Arm() => Volatile.Write(ref _armed, 1);

        /// <summary>Lets the parked call, and every later one, answer.</summary>
        internal void Release() => _ = _released.TrySetResult();
    }

    /// <summary>
    /// Local cache double whose memory apply of a replicated write fails until recovered, or stalls ignoring cancellation until released;
    /// it records the keys of the writes applied after recovery.
    /// </summary>
    [ThreadSafe]
    internal sealed class ScriptedApplyCache : ILogicalNamespacedCache<object?>
    {
        private readonly TaskCompletionSource _applyEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _applyReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ApplyMode _mode;
        private readonly VolatileBool _recovered = new();
        private int _applyAttempts;
        private int _entryReads;

        internal ScriptedApplyCache(ApplyMode mode)
        {
            _mode = mode;
        }

        internal ConcurrentQueue<string> Applied { get; } = new();

        /// <summary>Gets the number of memory applies started, whether they stalled, failed, or succeeded.</summary>
        internal int ApplyAttempts => Volatile.Read(ref _applyAttempts);

        internal Task ApplyEntered => _applyEntered.Task;

        /// <summary>Gets the number of entry reads, which the prepare of a conditional write issues.</summary>
        internal int EntryReads => Volatile.Read(ref _entryReads);

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _entryReads);
            return ValueTask.FromResult<NodeCacheEntry<object?>?>(null);
        }

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref _applyAttempts);
            _ = _applyEntered.TrySetResult();
            if (_mode == ApplyMode.Stall)
                return new ValueTask(_applyReleased.Task);
            if (_mode == ApplyMode.StallThenFail && attempt == 1)
                return new ValueTask(FailAfterReleaseAsync());
            if (!_recovered.Read())
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));

            Applied.Enqueue(key);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        internal void Recover() => _recovered.Write(true);

        internal void ReleaseApply() => _ = _applyReleased.TrySetResult();

        private async Task FailAfterReleaseAsync()
        {
            await new ValueTask(_applyReleased.Task).ConfigureAwait(false);
            throw new InvalidOperationException("Injected memory apply failure after the majority.");
        }
    }

    /// <summary>
    /// Journal lifecycle double whose startup gate is open from the start or opens when released; it signals the first wait for it and
    /// honors cancellation of that wait.
    /// </summary>
    [ThreadSafe]
    internal sealed class RecoveryLifecycle : IJournalCoordinatorLifecycle
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _requested = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private RecoveryLifecycle()
        {
        }

        public event EventHandler? OnAppended
        {
            add => _ = value;
            remove => _ = value;
        }

        public int CurrentSegmentIndex => 0;

        public bool IsJournalGroupCommitEnabled => false;

        public ulong NextSequence => 1;

        /// <summary>Gets a task that completes when a caller waited for the startup gate.</summary>
        public Task Requested => _requested.Task;

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken)
        {
            _ = _requested.TrySetResult();
            return new ValueTask(_released.Task.WaitAsync(cancellationToken));
        }

        /// <summary>Creates a lifecycle whose startup gate is already open.</summary>
        /// <returns>The lifecycle.</returns>
        internal static RecoveryLifecycle Recovered()
        {
            var lifecycle = new RecoveryLifecycle();
            lifecycle.Release();
            return lifecycle;
        }

        /// <summary>Creates a lifecycle whose startup gate fails every wait with <paramref name="error" />.</summary>
        /// <param name="error">The failure of the startup gate.</param>
        /// <returns>The lifecycle.</returns>
        internal static RecoveryLifecycle Failed(Exception error)
        {
            var lifecycle = new RecoveryLifecycle();
            _ = lifecycle._released.TrySetException(error);
            return lifecycle;
        }

        /// <summary>Creates a lifecycle whose startup gate stays closed until <see cref="Release" />.</summary>
        /// <returns>The lifecycle.</returns>
        internal static RecoveryLifecycle Recovering() => new();

        /// <summary>Opens the startup gate.</summary>
        internal void Release() => _ = _released.TrySetResult();
    }
}
