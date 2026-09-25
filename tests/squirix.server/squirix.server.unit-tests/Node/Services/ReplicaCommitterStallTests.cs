using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=2 group owner whose memory apply fails or stalls after the majority acknowledged the write.</summary>
public sealed class ReplicaCommitterStallTests : IsolatedStorageTestBase
{
    private const int CommitUnknownEventId = 4006;
    private const int LeakedOnShutdownEventId = 4005;

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private enum ApplyMode
    {
        Fail = 0,
        Stall = 1,
    }

    /// <summary>
    /// A write whose outcome is unknown after its local append reaches the pipeline as the stable commit-unknown contract (gRPC
    /// Unavailable with COMMIT_OUTCOME_UNKNOWN), and the domain error mapping keeps it that way instead of turning it into a precondition failure.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicatedUnknownMapsToStableContract(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        var log = new LeakRecordingLogger();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, log);
        var cache = new DomainErrorMappingCacheDecorator<object?>(new ReplicatedCache(local, committer));

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cache.SetEntryAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        var transport = error.ToRpcException();
        var forwarded = new DomainErrorMappingCacheDecorator<object?>(new ForwardedUnknownCache());
        var remote = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarded.SetEntryAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(transport.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(transport.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(remote.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(remote.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(log.UnknownLevel).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.UnknownCause?.InnerException?.Message).IsEqualTo("Injected memory apply failure after the majority.");
    }

    /// <summary>
    /// Disposing the committer while a write past its majority is stuck in the memory apply returns within the shutdown budget, logs the
    /// leak, refuses new writes, and leaves the in-flight write to complete once the apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitterDisposeBoundedUnderStall(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Stall);
        var log = new LeakRecordingLogger();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        var committer = CreateCommitter(registry, local, log);
        try
        {
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var refused = committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(refused);
            _ = await Assert.That(log.LeakCount).IsEqualTo(1);

            local.ReleaseApply();
            await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            local.ReleaseApply();
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// While a committed entry is still unapplied, a new write is refused before its local append with a definite, retryable error
    /// (not COMMIT_OUTCOME_UNKNOWN), because its outcome would be prepared from memory that misses the entry; once the apply recovers,
    /// the retried write applies after the entry, in log order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingApplyRefusesWriteDefinitely(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken));
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(refused.ToRpcException().StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken);
        await SequenceAssert.EqualAsync(["k1", "k2"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A committer resync (the group lost and regained its write majority) applies the old coordinator's committed-but-unapplied entry
    /// before replacing the coordinator, instead of dropping it together with the coordinator.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResyncDrivesCommittedPendingApply(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        await FailFirstWriteThenDropMajorityAsync(committer, registry, cancellationToken);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry(), cancellationToken);

        await SequenceAssert.EqualAsync(["k1", "k3"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A committer resync whose committed entry still cannot be applied refuses the write definitely and keeps the old coordinator with
    /// the entry, so the next resync after the apply recovers applies it before any newer write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResyncKeepsPendingApply(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        await FailFirstWriteThenDropMajorityAsync(committer, registry, cancellationToken);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry(), cancellationToken));
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry(), cancellationToken);
        await SequenceAssert.EqualAsync(["k1", "k4"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>Commits a write whose apply fails after its majority, then demotes the follower so the next write drops the started state.</summary>
    /// <param name="committer">The committer under test.</param>
    /// <param name="registry">The registry owning the group log and eligibility.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task FailFirstWriteThenDropMajorityAsync(ReplicaGroupCommitter committer, ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        // Invalid progress demotes a ready follower to catching up: the next write has no verified majority and drops the started
        // state, so the write after it resyncs (a new coordinator whose start re-verifies the follower).
        _ = registry.EligibilityFor("n1").TryMarkCatchingUp(1, default);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken));
    }

    private static async Task<ulong> LastLogIndexAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        return status.LastLogIndex;
    }

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ILogicalNamespacedCache<object?> local, ILogger? log = null) =>
        new(registry, new TwoNodeLocator(), new AcceptingGateway(), local, "n1", Fingerprint, 1)
        {
            Log = log ?? new LeakRecordingLogger(),
            ShutdownBudget = TimeSpan.FromMilliseconds(200),
        };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(Dir, ["n1"], 2, Fingerprint, 1);
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    [Immutable]
    private sealed class TwoNodeLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 2;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            destination[1] = "n2";
        }
    }

    /// <summary>Follower double that always holds the leader batch.</summary>
    [Immutable]
    private sealed class AcceptingGateway : IReplicaRpcGateway
    {
        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last));
        }
    }

    /// <summary>Remote owner double that reports the stable commit-unknown contract over gRPC.</summary>
    [Immutable]
    private sealed class ForwardedUnknownCache : ILogicalNamespacedCache<object?>
    {
        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => throw Unknown();

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => throw Unknown();

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw Unknown();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw Unknown();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            ValueTask.FromException(Unknown());

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => throw Unknown();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            throw Unknown();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => throw Unknown();

        private static RpcException Unknown() => ServerOpContract.CommitOutcomeUnknown().ToRpcException();
    }

    /// <summary>Logger double counting the committer's shutdown leak event and capturing its commit-unknown warning.</summary>
    [ThreadSafe]
    private sealed class LeakRecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<EventId> _events = new();
        private readonly ConcurrentQueue<(LogLevel Level, Exception? Cause)> _unknowns = new();

        internal int LeakCount
        {
            get
            {
                var count = 0;
                foreach (var eventId in _events)
                {
                    if (eventId.Id == LeakedOnShutdownEventId)
                        count++;
                }

                return count;
            }
        }

        internal Exception? UnknownCause => _unknowns.TryPeek(out var unknown) ? unknown.Cause : null;

        internal LogLevel? UnknownLevel => _unknowns.TryPeek(out var unknown) ? unknown.Level : null;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _events.Enqueue(eventId);
            if (eventId.Id != CommitUnknownEventId)
                return;

            _unknowns.Enqueue((logLevel, exception));
        }
    }

    /// <summary>
    /// Local cache double whose memory apply of a replicated write fails until recovered, or stalls ignoring cancellation until released;
    /// it records the keys of the writes applied after recovery.
    /// </summary>
    [ThreadSafe]
    private sealed class ScriptedApplyCache : ILogicalNamespacedCache<object?>
    {
        private readonly TaskCompletionSource _applyEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _applyReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ApplyMode _mode;
        private readonly VolatileBool _recovered = new();

        internal ScriptedApplyCache(ApplyMode mode)
        {
            _mode = mode;
        }

        internal ConcurrentQueue<string> Applied { get; } = new();

        internal Task ApplyEntered => _applyEntered.Task;

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult<NodeCacheEntry<object?>?>(null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _ = _applyEntered.TrySetResult();
            if (_mode == ApplyMode.Stall)
                return new ValueTask(_applyReleased.Task);
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
    }
}
