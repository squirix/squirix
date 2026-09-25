using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
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
    }

    internal static string NewOperationId() => Guid.NewGuid().ToString("N");

    internal static string TailOperationId(string key) => "tail-" + key;

    internal static NodeCacheEntry<object?> Entry(string key) => new() { Value = key, Version = 1 };

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway) => CreateCommitter(registry, gateway, new StubCache());

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, StubCache cache, ILogger? log = null) =>
        new(registry, new ThreeNodeLocator(), gateway, cache, "n1", Fingerprint, 1) { Log = log ?? NullLogger.Instance };

    internal static Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, CancellationToken cancellationToken) => OpenRegistryAsync(dir, null, cancellationToken);

    internal static async Task<ReplicaGroupRegistry> OpenRegistryAsync(string dir, FollowerLogOptions? options, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, ["n1"], 3, Fingerprint, 1, options);
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

    /// <summary>Commits one write on a fresh group, leaving durable RF=3 progress on disk for the restart under test.</summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task SeedAsync(string dir, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        await committer.CommitSetAsync(NewOperationId(), "cache", "k0", new NodeCacheEntry<object?> { Value = "v0", Version = 1 }, cancellationToken);
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
    internal static async Task SeedTailAsync(string dir, ulong currentTerm, CancellationToken cancellationToken, params string[] keys)
    {
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1);
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

        /// <summary>Gets the node, predecessor index, and entry count of every non-empty batch sent.</summary>
        internal ConcurrentQueue<(string Node, ulong PrevLogIndex, int Count)> Appends { get; } = new();

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            var mode = _modes.TryGetValue(nodeId, out var scripted) ? scripted : FollowerMode.Match;
            if (batch.Records.Count > 0)
                Appends.Enqueue((nodeId, batch.PrevLogIndex, batch.Records.Count));

            return mode switch
            {
                FollowerMode.Match => Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last)),
                FollowerMode.Mismatch => Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, batch.LeaderTerm, 0)),
                FollowerMode.Down => Task.FromException<FollowerLogAppendResult>(new IOException("follower is down")),
                FollowerMode.Longer => Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last + 5)),
                FollowerMode.Refused => Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.StaleTerm, batch.LeaderTerm + 1, last)),
                FollowerMode.Behind => Task.FromResult(AppendBehind(nodeId, batch, last)),
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

        private FollowerLogAppendResult AppendBehind(string nodeId, FollowerBatch batch, ulong last)
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
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            Applied.Enqueue(key);
            return ValueTask.FromResult(_entries.TryAdd(key, entry));
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
