using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of a value read of a live entry on the owner of a group it leads, against the local reads under it. The replicated read
/// checks the entry is not expired on the leader from the value and deadline it reads, without building the entry: it is expected to allocate
/// nothing, as the local value read does, and the local entry read allocates only the entry it returns. The CI allocation guard checks
/// these allocations against alloc-ci-baseline.json.
/// Node n1 owns and leads group n1; its followers n2 and n3 accept every batch.
/// </summary>
[MemoryDiagnoser]
public class ReplicatedCacheValueReadBenchmarks
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const int OperationsPerInvoke = 100_000;
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private ReplicaGroupCommitters? _committers;
    private TempDirectory? _dir;
    private ClientCache<object?>? _local;
    private ReplicatedCache? _replicated;
    private ReplicaGroupRegistry? _registry;

    /// <summary>Disposes the committers, the registry and the data directory.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_committers != null)
            await _committers.DisposeAsync().ConfigureAwait(false);
        if (_registry != null)
            await _registry.DisposeAsync().ConfigureAwait(false);
        _dir?.Dispose();
        _committers = null;
        _registry = null;
        _dir = null;
        _local = null;
        _replicated = null;
    }

    /// <summary>Reads the entry from the local cache, the read under the replicated one.</summary>
    /// <returns>A task that completes when all reads finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerInvoke)]
    public async Task LocalEntryReadAsync()
    {
        var local = ThrowHelper.Required(_local, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await local.GetEntryAsync(CacheName, Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads the value from the local cache, the value read under the replicated one.</summary>
    /// <returns>A task that completes when all reads finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task LocalValueReadAsync()
    {
        var local = ThrowHelper.Required(_local, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await local.GetValueAsync(CacheName, Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads the value through the replicated cache, as the leader of the group.</summary>
    /// <returns>A task that completes when all reads finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ReplicatedValueReadAsync()
    {
        var replicated = ThrowHelper.Required(_replicated, "Benchmark cache was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await replicated.GetValueAsync(CacheName, Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Opens the seeded group, commits the entry the reads hit, and seeds the copy with a first read.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("replicated-value-read-bench");
        await SeededLogs.SeedGroupsAsync(_dir, ["n1"], CancellationToken.None).ConfigureAwait(false);
        var logOptions = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        _registry = new ReplicaGroupRegistry(_dir, ["n1"], 3, Fingerprint, 1, NullLoggerFactory.Instance, logOptions) { ElectionClock = TimeProvider.System };
        await _registry.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        _local = new ClientCache<object?>(physical, physical);
        var led = new ReplicaGroupCommitter[1];
        led[0] = CreateCommitter(_registry, _local);
        _committers = new ReplicaGroupCommitters(led, "n1", new NoOwners(), TimeProvider.System);
        await _committers.For("n1").CommitSetAsync("op-1", CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, TimeSpan.FromDays(1)), CancellationToken.None)
            .ConfigureAwait(false);
        _replicated = new ReplicatedCache(_local, _committers);
        _ = await _replicated.GetValueAsync(CacheName, Key, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Creates the committer of group n1, which node n1 owns; the committers dispose it.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="local">The local cache the group applies to.</param>
    /// <returns>The committer.</returns>
    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ClientCache<object?> local) =>
        new(registry, new ThreeNodeLocator(), new AcceptingGateway(), local, ("n1", "n1"), new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = new RecoveredLifecycle(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, "n1", "n1"),
            Clock = TimeProvider.System,
        };

    /// <summary>Follower transport double: every follower holds the leader log and accepts every batch.</summary>
    [Immutable]
    private sealed class AcceptingGateway : IReplicaRpcGateway
    {
        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last));
        }
    }

    /// <summary>Owner lookup the benchmark never asks: the only key belongs to the group this node owns.</summary>
    [Immutable]
    private sealed class NoOwners : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => throw new InvalidOperationException("The benchmark reads only keys of its own group.");
    }

    /// <summary>Journal lifecycle whose startup gate is open.</summary>
    [Immutable]
    private sealed class RecoveredLifecycle : IJournalCoordinatorLifecycle
    {
        public event EventHandler? OnAppended
        {
            add => _ = value;
            remove => _ = value;
        }

        public int CurrentSegmentIndex => 0;

        public bool IsJournalGroupCommitEnabled => false;

        public ulong NextSequence => 1;

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    /// <summary>Places every group on nodes n1, n2 and n3.</summary>
    [Immutable]
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
