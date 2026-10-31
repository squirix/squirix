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
/// Cost of the memory pressure check a replicated write makes before it grows memory, on the owner of a group with no pressure reported.
/// The check reads the pressure record of the committer: it is expected to allocate nothing.
/// </summary>
[MemoryDiagnoser]
public class ReplicaMemberPressureBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private ReplicaGroupCommitters? _committers;
    private TempDirectory? _dir;
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
    }

    /// <summary>Checks the memory pressure of the group the way a replicated write does.</summary>
    /// <returns>Whether any member reported pressure; always <see langword="false" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public bool HasMemberPressure()
    {
        var committers = ThrowHelper.Required(_committers, "Benchmark committers were not initialized.");
        var any = false;
        for (var i = 0; i < OperationsPerInvoke; i++)
            any |= committers.HasMemberPressure("cache", "k");

        return any;
    }

    /// <summary>Opens the group and creates the committer of the group n1, which node n1 owns.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("member-pressure-bench");
        await SeededLogs.SeedGroupsAsync(_dir, ["n1"], CancellationToken.None).ConfigureAwait(false);
        var logOptions = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        _registry = new ReplicaGroupRegistry(_dir, ["n1"], 3, Fingerprint, 1, NullLoggerFactory.Instance, logOptions) { ElectionClock = TimeProvider.System };
        await _registry.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        var led = new ReplicaGroupCommitter[1];
        led[0] = new ReplicaGroupCommitter(
            _registry,
            new ThreeNodeLocator(),
            new NoGateway(),
            local,
            ("n1", "n1"),
            new ReplicaTopologyStamp(Fingerprint, 1),
            NullLogger<ReplicaGroupCommitter>.Instance)
        {
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = new RecoveredLifecycle(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, "n1", "n1"),
            Clock = TimeProvider.System,
        };
        _committers = new ReplicaGroupCommitters(led, "n1", new NoOwners(), TimeProvider.System);
    }

    /// <summary>Follower transport double the benchmark never calls.</summary>
    [Immutable]
    private sealed class NoGateway : IReplicaRpcGateway
    {
        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The benchmark sends no batch.");
    }

    /// <summary>Owner lookup the benchmark never asks: the committers own every key.</summary>
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
