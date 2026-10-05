using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of the leader re-applying a committed backlog of replica log entries to a journaled local cache, the way start, resync catch-up and
/// readiness do: every entry goes through <see cref="ReplicaLeaderApplier" /> on a task started without the execution context.
/// The reported time is per backlog, because the invocation count cannot take a parameter value: divide by <see cref="Backlog" /> for the per-entry cost.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class ReplicaLeaderReapplyBenchmarks
{
    private JournalBenchmarkHost? _host;
    private ReplicaLeaderApplier? _applier;
    private ILogicalNamespacedCache<object?>? _cache;
    private byte[][] _records = [];

    /// <summary>Gets or sets the number of committed entries re-applied per invocation.</summary>
    [Params(64, 512)]
    public int Backlog { get; set; }

    /// <summary>Gets or sets the journal group-commit maximum wait in milliseconds; 0 turns group commit off.</summary>
    [Params(0, 1, 5)]
    public int GroupCommitMaxWaitMs { get; set; }

    /// <summary>Disposes the journal coordinator and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _applier = null;
        _cache = null;
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Re-applies the backlog from a task started without the execution context.</summary>
    /// <returns>A task that completes when every entry is applied.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark]
    public async Task ReapplyBacklogAsync()
    {
        var applier = ThrowHelper.Required(_applier, "Benchmark applier was not initialized.");
        var records = _records;
        Task reapply;
        using (ExecutionContext.SuppressFlow())
            reapply = Task.Factory.StartNew(() => ReapplyAsync(applier, records), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

        await reapply.ConfigureAwait(false);
    }

    /// <summary>Creates the journal coordinator, the journaled cache and the pre-encoded backlog for the current parameter set.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(GroupCommitMaxWaitMs),
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("replica-reapply-bench", options, CancellationToken.None).ConfigureAwait(false);
        _cache = new JournalLoggingCacheDecorator<object?>(
            new InMemoryCache(),
            _host.Coordinator,
            new DurableMutationExecutor(_host.Coordinator, NullLogger<DurableMutationExecutor>.Instance));
        var entry = ReplicaCacheApplier.EncodeEntry(new NodeCacheEntry<object?> { Value = "v", Version = 1 });
        _records = new byte[Backlog][];
        var logIndex = 0UL;
        for (var i = 0; i < _records.Length; i++)
        {
            logIndex++;
            var record = new ReplicaLogRecord(
                logIndex,
                1UL,
                "op-" + NodeInvariantIndexStrings.Format(i),
                "client",
                new byte[] { 1 },
                "UserMutation",
                "bench",
                Encoding.UTF8.GetBytes("k" + NodeInvariantIndexStrings.Format(i)),
                ReplicaMutationKinds.Set,
                entry,
                ReplicaOutcomeCodec.Encode(true, ReadOnlyMemory<byte>.Empty),
                0,
                0,
                0,
                0);
            _records[i] = ReplicaLogCodec.Encode(in record);
        }
    }

    /// <summary>Creates an applier whose applied index starts at zero, so each invocation re-applies the whole backlog.</summary>
    [IterationSetup]
    public void IterationSetup() =>
        _applier = new ReplicaLeaderApplier(ThrowHelper.Required(_cache, "Benchmark cache was not initialized."), NullLogger.Instance);

    private static async Task ReapplyAsync(ReplicaLeaderApplier applier, byte[][] records)
    {
        var logIndex = 0UL;
        foreach (var record in records)
        {
            logIndex++;
            await applier.ApplyAsync(logIndex, record, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Memory side of the journaled cache: the decorator under measurement writes the journal frame, this stores the entry.</summary>
    [ThreadSafe]
    private sealed class InMemoryCache : ILogicalNamespacedCache<object?>
    {
        private readonly ConcurrentDictionary<CacheKey, NodeCacheEntry<object?>> _store = new();

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(_store.TryGetValue(new CacheKey(cacheName, key), out var entry) ? entry : null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                _store.TryGetValue(new CacheKey(cacheName, key), out var entry) ? new NodeCacheValueResult<object?>(true, entry.Value) : new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                _store.TryRemove(new CacheKey(cacheName, key), out var entry) ? new CacheRemoveResult<object?>(true, entry.Value) : new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _store[new CacheKey(cacheName, key)] = entry;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(_store.TryAdd(new CacheKey(cacheName, key), entry));

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }
}
