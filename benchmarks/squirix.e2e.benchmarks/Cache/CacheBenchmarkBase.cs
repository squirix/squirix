using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Cluster;
using Squirix.E2EBenchmarks.Support.Harness;
using Squirix.E2EBenchmarks.Support.Runtime;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Shared setup and cleanup for parameterized E2E benchmark classes.</summary>
public abstract class CacheBenchmarkBase
{
    /// <summary>
    /// Number of cache operations performed per benchmark invocation.
    /// </summary>
    protected const int BatchSize = 32;

    private int _addOffset;
    private int _expiringHitOffset;
    private int _hitOffset;
    private int _missOffset;
    private int _uniqueAddOffset;

    /// <summary>Gets or sets the scenario measured by the current BenchmarkDotNet case.</summary>
    [ParamsSource(nameof(Scenarios))]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global", Justification = "A property annotated with [ParamsSource] must have a public setter")]
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global", Justification = "A property annotated with [ParamsSource] must have a public setter")]
    public BenchmarkScenario Scenario { get; set; } = BenchmarkScenario.CreateDefaultMatrix()[0];

    /// <summary>Gets the scenario matrix used by BenchmarkDotNet.</summary>
    public virtual IEnumerable<BenchmarkScenario> Scenarios => BenchmarkScenario.CreateDefaultMatrix();

    /// <summary>Gets the typed value adapter for the active value shape.</summary>
    private protected IE2EBenchmarkValueAdapter Adapter { get; private set; } = UninitializedBenchmarkValueAdapter.Instance;

    /// <summary>Gets the consumer used to prevent dead-code elimination.</summary>
    private protected Consumer Consumer { get; } = new();

    /// <summary>Gets the keyspace for the active topology.</summary>
    private protected E2EBenchmarkKeyspace Keyspace { get; private set; } = E2EBenchmarkKeyspace.Create("benchmark", BenchmarkTopology.SingleNode);

    private E2EBenchmarkCluster? Cluster { get; set; }

    /// <summary>Stops the real Squirix cluster.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (Cluster is not null)
            await Cluster.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Starts the real Squirix cluster, opens the public client, and seeds hit keys.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        BenchmarkRuntime.EnsureInitialized();
        var cacheName = GetType().Name + "-" + Scenario;
        Keyspace = E2EBenchmarkKeyspace.Create(cacheName, Scenario.Topology);
        Cluster = await E2EBenchmarkCluster.StartAsync(Scenario.Topology, Scenario.DurabilityMode, CancellationToken.None).ConfigureAwait(false);
        Adapter = await E2EBenchmarkValueAdapter.CreateAsync(Cluster, Scenario.ValueShape, cacheName, CancellationToken.None).ConfigureAwait(false);
        await Adapter.SeedAsync(Keyspace.HitKeys, CancellationToken.None).ConfigureAwait(false);
        await SeedAdditionalStateAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Gets the next deterministic add key.</summary>
    /// <returns>A key from the add keyspace.</returns>
    protected string NextAddKey() => Keyspace.AddKey(Interlocked.Increment(ref _addOffset));

    /// <summary>Gets the next pre-seeded expiring hit key.</summary>
    /// <returns>A key from the expiring hit keyspace.</returns>
    protected string NextExpiringHitKey() => Keyspace.ExpiringHitKey(Interlocked.Increment(ref _expiringHitOffset));

    /// <summary>Gets the next deterministic hit key.</summary>
    /// <returns>A key from the hit keyspace.</returns>
    protected string NextHitKey() => Scenario.Topology is BenchmarkTopology.TwoNodeHotKeys ? Keyspace.HotKey(Interlocked.Increment(ref _hitOffset))
        : Keyspace.HitKey(Interlocked.Increment(ref _hitOffset));

    /// <summary>Gets the next deterministic miss key.</summary>
    /// <returns>A key from the miss keyspace.</returns>
    protected string NextMissKey() => Keyspace.MissKey(Interlocked.Increment(ref _missOffset));

    /// <summary>Gets the next globally unique add key for benchmark paths that require missing keys across all BenchmarkDotNet iterations.</summary>
    /// <returns>A key that has not been returned by this benchmark instance before.</returns>
    protected string NextUniqueAddKey() => string.Concat("unique:add:", Interlocked.Increment(ref _uniqueAddOffset).ToString("D10", CultureInfo.InvariantCulture));

    /// <summary>Allows derived benchmark classes to seed state that is specific to their pure-operation benchmark methods.</summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when additional setup is finished.</returns>
    protected virtual Task SeedAdditionalStateAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private sealed class UninitializedBenchmarkValueAdapter : IE2EBenchmarkValueAdapter
    {
        internal static readonly UninitializedBenchmarkValueAdapter Instance = new();

        public Task AddAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException(NotInitialized());

        public Task<bool> AddConflictAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetEntryHitAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetExpirationAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetOrAddHitAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetOrAddMissAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetValueHitAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> GetValueMissAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task SeedAsync(string[] keys, CancellationToken cancellationToken) =>
            Task.FromException(NotInitialized());

        public Task SeedExpiringAsync(string[] keys, TimeSpan expiration, CancellationToken cancellationToken) =>
            Task.FromException(NotInitialized());

        public Task SetAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException(NotInitialized());

        public Task SetExpiringAsync(string key, int valueIndex, TimeSpan expiration, CancellationToken cancellationToken) =>
            Task.FromException(NotInitialized());

        public Task<bool> TouchAbsoluteAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> TouchRelativeAsync(string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> TryAddAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        public Task<bool> UpdateAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            Task.FromException<bool>(NotInitialized());

        private static InvalidOperationException NotInitialized() =>
            new("E2E benchmark adapter is initialized in GlobalSetup only.");
    }
}
