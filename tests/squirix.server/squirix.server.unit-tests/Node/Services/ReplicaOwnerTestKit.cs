using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>RF=3 group owner harness: node n1 owns the group, n2 and n3 are scripted followers.</summary>
internal static class ReplicaOwnerTestKit
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>How a scripted follower answers leader appends.</summary>
    internal enum FollowerMode
    {
        /// <summary>Holds exactly the leader log and accepts every batch.</summary>
        Match = 0,

        /// <summary>Refuses every batch as a log mismatch.</summary>
        Mismatch = 1,

        /// <summary>Fails the transport.</summary>
        Down = 2,

        /// <summary>Accepts every batch but reports a log five entries longer.</summary>
        Longer = 3,

        /// <summary>Refuses every batch for a reason unrelated to its log.</summary>
        Refused = 4,

        /// <summary>Holds the leader log only through a scripted index, and then every batch it accepts.</summary>
        Behind = 5,

        /// <summary>Refuses the probe as a log mismatch, accepts a batch with entries but reports a log five entries longer.</summary>
        BehindLonger = 6,

        /// <summary>Answers a batch with entries only by observing its cancellation, as a follower that is slow but not down.</summary>
        Silent = 7,
    }

    internal static string NewOperationId() => Guid.NewGuid().ToString("N");

    internal static string TailOperationId(string key) => "tail-" + key;

    internal static NodeCacheEntry<object?> Entry(string key) => new() { Value = key, Version = 1 };

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway) => CreateCommitter(registry, gateway, new StubCache());

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, ILogicalNamespacedCache<object?> cache, ILogger<ReplicaGroupCommitter>? log = null) =>
        new(registry, new ThreeNodeLocator(), gateway, cache, "n1", new ReplicaTopologyStamp(Fingerprint, 1), log ?? NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
        };

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, TimeSpan commitBudget) =>
        new(registry, new ThreeNodeLocator(), gateway, new StubCache(), "n1", new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            CommitBudget = commitBudget,
        };

    internal static ReplicaGroupCommitter CreateCommitter(
        ReplicaGroupRegistry registry,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> cache,
        TimeProvider clock,
        ReplicationMetrics? metrics = null) =>
        new(registry, new ThreeNodeLocator(), gateway, cache, "n1", new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance) { Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), Clock = clock, Metrics = metrics };

    /// <summary>Creates a committer on one clock for its decisions and budgets, whose dispose drain runs on its own clock and budget.</summary>
    /// <param name="registry">Replica group registry of the owner.</param>
    /// <param name="gateway">Follower transport double.</param>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="clock">The clock of the decisions, the commit budget and the follower request timeouts.</param>
    /// <param name="log">The committer logger.</param>
    /// <param name="shutdown">The clock and the length of the shutdown budget.</param>
    /// <returns>The committer.</returns>
    internal static ReplicaGroupCommitter CreateCommitter(
        ReplicaGroupRegistry registry,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> cache,
        TimeProvider clock,
        ILogger<ReplicaGroupCommitter> log,
        (TimeProvider Clock, TimeSpan Budget) shutdown) =>
        new(registry, new ThreeNodeLocator(), gateway, cache, "n1", new ReplicaTopologyStamp(Fingerprint, 1), log)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            Clock = clock,
            BudgetTimeProvider = clock,
            ShutdownTimeProvider = shutdown.Clock,
            ShutdownBudget = shutdown.Budget,
        };

    /// <summary>Creates a committer whose commit budget and follower request timeouts run on <paramref name="budgetClock" />.</summary>
    /// <param name="registry">Replica group registry of the owner.</param>
    /// <param name="gateway">Follower transport double.</param>
    /// <param name="budgetClock">The time source of the commit budget and of the follower request timeouts.</param>
    /// <returns>The committer.</returns>
    internal static ReplicaGroupCommitter CreateCommitterOnBudgetClock(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, TimeProvider budgetClock) =>
        new(registry, new ThreeNodeLocator(), gateway, new StubCache(), "n1", new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            BudgetTimeProvider = budgetClock,
        };

    /// <summary>Creates a committer whose follower request timeouts run on <paramref name="budgetClock" />, under short commit and shutdown budgets.</summary>
    /// <param name="registry">Replica group registry of the owner.</param>
    /// <param name="gateway">Follower transport double.</param>
    /// <param name="budgetClock">The time source of the commit budget and of the follower request timeouts.</param>
    /// <param name="commitBudget">The commit budget, which also bounds the drain of the senders when the coordinator is replaced.</param>
    /// <param name="shutdownBudget">The shutdown budget, which also bounds the teardown of the senders and the coordinator it replaces.</param>
    /// <returns>The committer.</returns>
    internal static ReplicaGroupCommitter CreateCommitterOnBudgetClock(
        ReplicaGroupRegistry registry,
        IReplicaRpcGateway gateway,
        TimeProvider budgetClock,
        TimeSpan commitBudget,
        TimeSpan shutdownBudget) =>
        new(registry, new ThreeNodeLocator(), gateway, new StubCache(), "n1", new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            BudgetTimeProvider = budgetClock,
            CommitBudget = commitBudget,
            ShutdownBudget = shutdownBudget,
        };

    internal static Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, CancellationToken cancellationToken) => OpenRegistryAsync(dir, null, cancellationToken);

    internal static Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, FollowerLogOptions? options, CancellationToken cancellationToken) =>
        OpenRegistryAsync(dir, ["n1"], options, cancellationToken);

    /// <summary>Opens a registry serving <paramref name="groupIds" />: the owned group n1 and the groups this node follows.</summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="groupIds">The served group identifiers.</param>
    /// <param name="options">Follower log options of every group log.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The opened registry.</returns>
    internal static async Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, string[] groupIds, FollowerLogOptions? options, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, groupIds, 3, Fingerprint, 1, NullLoggerFactory.Instance, options);
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

    /// <summary>
    /// Commits one write on a fresh group and persists its applied index, leaving durable RF=3 progress on disk for the restart under
    /// test, as an owner whose applied-index flush ran before it stopped: the restarted owner applies nothing of it again.
    /// </summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The owned group log is not open or refused the applied index.</exception>
    internal static async Task SeedAsync(string dir, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await committer.CommitSetAsync(NewOperationId(), "cache", "k0", new NodeCacheEntry<object?> { Value = "v0", Version = 1 }, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var applied = await log.AdvanceAppliedAsync(status.CommitIndex, cancellationToken);
        if (!applied.Success)
            throw new InvalidOperationException($"The owned group log refused the applied index: {applied.RefusalCode}.");
    }

    /// <summary>
    /// Appends conditional adds of the given keys to the owned group log without committing them, as a leader that crashed between
    /// the local append and the commit leaves them, then raises the log's current term.
    /// </summary>
    /// <param name="dir">Node data directory holding the seeded, committed group.</param>
    /// <param name="currentTerm">Current term of the log after the tail; above one, the tail is of an older term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="keys">Keys of the uncommitted conditional adds, in log order.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The owned group log is not open or refused the tail.</exception>
    internal static Task SeedTailAsync(string dir, ulong currentTerm, CancellationToken cancellationToken, params string[] keys) =>
        SeedTailAsync(dir, currentTerm, new StubCache(), cancellationToken, keys);

    /// <summary>
    /// Appends conditional adds of the given keys to the owned group log without committing them, each decided against
    /// <paramref name="decisionCache" />, then raises the log's current term.
    /// </summary>
    /// <param name="dir">Node data directory holding the seeded, committed group.</param>
    /// <param name="currentTerm">Current term of the log after the tail; above one, the tail is of an older term.</param>
    /// <param name="decisionCache">The memory the leader read when it decided the adds: a key it holds makes its add decide false.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="keys">Keys of the uncommitted conditional adds, in log order.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The owned group log is not open or refused the tail.</exception>
    internal static async Task SeedTailAsync(string dir, ulong currentTerm, ILogicalNamespacedCache<object?> decisionCache, CancellationToken cancellationToken, params string[] keys)
    {
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var factory = new ReplicaMutationFactory(decisionCache, "n1", 1, TimeProvider.System, NullLogger.Instance);
        var index = status.LastLogIndex;
        foreach (var key in keys)
        {
            var mutation = await factory.PrepareTryAddAsync(TailOperationId(key), "cache", key, Entry(key), index + 1, cancellationToken);
            FollowerLogEntry[] entry = [new(mutation.LogIndex, mutation.Term, mutation.CanonicalPayload)];
            var appended = await log.AppendAsync(new FollowerLogAppendRequest("n1", 1, index, 1, status.CommitIndex, entry), cancellationToken);
            if (!appended.Success)
                throw new InvalidOperationException($"The owned group log refused the tail entry: {appended.RefusalCode}.");

            index++;
        }

        if (currentTerm > 1)
            _ = await log.AppendAsync(new FollowerLogAppendRequest("n1", currentTerm, index, 1, status.CommitIndex, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
    }

    /// <summary>Appends one prepared record to the owned group log as the next entry, committed or as an uncommitted tail, without applying it.</summary>
    /// <param name="dir">Node data directory holding the seeded, committed group.</param>
    /// <param name="record">The record; its log index and term are replaced by the next position of the log.</param>
    /// <param name="committed">Whether the commit index moves onto the appended entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The owned group log is not open or refused the entry.</exception>
    internal static async Task SeedRecordAsync(string dir, ReplicaLogRecord record, bool committed, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var index = status.LastLogIndex + 1;
        var positioned = record with { LogIndex = index, Term = 1 };
        FollowerLogEntry[] entry = [new(index, 1, ReplicaLogCodec.Encode(in positioned))];
        var appended = await log.AppendAsync(new FollowerLogAppendRequest("n1", 1, status.LastLogIndex, 1, committed ? index : status.CommitIndex, entry), cancellationToken);
        if (!appended.Success)
            throw new InvalidOperationException($"The owned group log refused the entry: {appended.RefusalCode}.");
    }

    /// <summary>Raises the owned group log's current term without appending an entry, as a new leader term that has not written yet.</summary>
    /// <param name="registry">The open registry of the owner.</param>
    /// <param name="currentTerm">The new current term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The owned group log is not open or refused the term.</exception>
    internal static async Task RaiseTermAsync(ReplicaGroupRegistry registry, ulong currentTerm, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var raised = await log.AppendAsync(
            new FollowerLogAppendRequest("n1", currentTerm, status.LastLogIndex, status.LastLogTerm, status.CommitIndex, ReadOnlyMemory<FollowerLogEntry>.Empty),
            cancellationToken);
        if (!raised.Success)
            throw new InvalidOperationException($"The owned group log refused term {currentTerm}: {raised.RefusalCode}.");
    }

    internal static async Task<FollowerLogStatus> StatusAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        registry.TryGetLog("n1", out var log) ? await log.GetStatusAsync(cancellationToken)
            : throw new InvalidOperationException("The owned group log is not open.");

    /// <summary>
    /// Follower double: matches the leader batch, refuses it as a log mismatch, or fails the transport. A follower behind the leader
    /// holds its log only through a scripted index: it refuses a batch whose predecessor it lacks (the probe at the leader's last
    /// entry) and accepts one whose predecessor it holds (the re-sent tail), then holds that batch too.
    /// </summary>
    internal sealed class ScriptedGateway : IReplicaRpcGateway
    {
        private readonly ConcurrentDictionary<string, ulong> _held = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, FollowerMode> _modes = new(StringComparer.Ordinal);

        /// <summary>Gets or sets a hook that runs, before it answers, on every batch with entries a follower receives.</summary>
        internal Action? OnAppend { get; set; }

        /// <summary>Gets the node, predecessor index, and entry count of every non-empty batch sent.</summary>
        internal ConcurrentQueue<(string Node, ulong PrevLogIndex, int Count)> Appends { get; } = new();

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            var mode = _modes.TryGetValue(nodeId, out var scripted) ? scripted : FollowerMode.Match;
            if (batch.Records.Count > 0)
            {
                Appends.Enqueue((nodeId, batch.PrevLogIndex, batch.Records.Count));
                OnAppend?.Invoke();
            }

            return mode switch
            {
                FollowerMode.Match => Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last)),
                FollowerMode.Mismatch => Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, batch.LeaderTerm, 0)),
                FollowerMode.Down => Task.FromException<FollowerLogAppendResult>(new IOException("follower is down")),
                FollowerMode.Longer => Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last + 5)),
                FollowerMode.Refused => Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, batch.LeaderTerm + 1, last)),
                FollowerMode.Behind => Task.FromResult(AppendBehind(nodeId, in batch, last)),
                FollowerMode.Silent when batch.Records.Count > 0 => new TaskCompletionSource<FollowerLogAppendResult>(TaskCreationOptions.RunContinuationsAsynchronously).Task.WaitAsync(cancellationToken),
                FollowerMode.Silent => Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last)),
                FollowerMode.BehindLonger => Task.FromResult(
                    batch.Records.Count == 0 ? new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, batch.LeaderTerm, 0)
                        : new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last + 5)),
                _ => throw new ArgumentOutOfRangeException(nameof(nodeId)),
            };
        }

        internal void Set(string nodeId, FollowerMode mode, ulong held = 0)
        {
            _modes[nodeId] = mode;
            _held[nodeId] = held;
        }

        private FollowerLogAppendResult AppendBehind(string nodeId, in FollowerBatch batch, ulong last)
        {
            var held = _held.TryGetValue(nodeId, out var scripted) ? scripted : 0;
            if (batch.PrevLogIndex > held)
                return new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, batch.LeaderTerm, held);

            held = Math.Max(held, last);
            _held[nodeId] = held;
            return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, held);
        }
    }

    /// <summary>In-memory local cache double that records the key of every applied write, in order.</summary>
    internal sealed class StubCache : ILogicalNamespacedCache<object?>
    {
        private readonly ConcurrentDictionary<string, NodeCacheEntry<object?>> _entries = new(StringComparer.Ordinal);

        internal ConcurrentQueue<string> Applied { get; } = new();

        /// <summary>Gets or sets a hook that runs after every applied write.</summary>
        internal Action? OnApplied { get; set; }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(_entries.TryGetValue(key, out var entry) ? entry : null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _entries[key] = entry;
            Applied.Enqueue(key);
            OnApplied?.Invoke();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            Applied.Enqueue(key);
            var added = _entries.TryAdd(key, entry);
            OnApplied?.Invoke();
            return ValueTask.FromResult(added);
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private sealed class ThreeNodeLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 3;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            destination[0] = "n1";
            destination[1] = "n2";
            destination[2] = "n3";
        }
    }
}
