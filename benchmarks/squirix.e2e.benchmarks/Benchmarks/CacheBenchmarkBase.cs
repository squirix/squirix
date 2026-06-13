using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.E2EBenchmarks.Harness;
using Squirix.E2EBenchmarks.Infrastructure;
using Squirix.E2EBenchmarks.Scenarios;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// Shared setup and cleanup for parameterized E2E benchmark classes.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Base class must remain public for BenchmarkDotNet benchmark classes.")]
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

    /// <summary>
    /// Gets or sets the scenario measured by the current BenchmarkDotNet case.
    /// </summary>
    [ParamsSource(nameof(Scenarios))]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global", Justification = "A property annotated with [ParamsSource] must have a public setter")]
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global", Justification = "A property annotated with [ParamsSource] must have a public setter")]
    public BenchmarkScenario Scenario { get; set; } = BenchmarkScenario.CreateDefaultMatrix()[0];

    /// <summary>
    /// Gets the scenario matrix used by BenchmarkDotNet.
    /// </summary>
    public virtual IEnumerable<BenchmarkScenario> Scenarios => BenchmarkScenario.CreateDefaultMatrix();

    /// <summary>
    /// Gets the typed value adapter for the active value shape.
    /// </summary>
    private protected IE2EBenchmarkValueAdapter Adapter { get; private set; } = null!;

    /// <summary>
    /// Gets the consumer used to prevent dead-code elimination.
    /// </summary>
    private protected Consumer Consumer { get; } = new();

    /// <summary>
    /// Gets the keyspace for the active topology.
    /// </summary>
    private protected E2EBenchmarkKeyspace Keyspace { get; private set; } = null!;

    private E2EBenchmarkCluster? Cluster { get; set; }

    /// <summary>
    /// Stops the real Squirix cluster.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when cleanup is finished.</returns>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (Cluster is not null)
            await Cluster.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Starts the real Squirix cluster, opens the public client, and seeds hit keys.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when setup is finished.</returns>
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

    /// <summary>
    /// Gets the next deterministic add key.
    /// </summary>
    /// <returns>A key from the add keyspace.</returns>
    protected string NextAddKey() => Keyspace.AddKey(Interlocked.Increment(ref _addOffset));

    /// <summary>
    /// Gets the next pre-seeded expiring hit key.
    /// </summary>
    /// <returns>A key from the expiring hit keyspace.</returns>
    protected string NextExpiringHitKey() => Keyspace.ExpiringHitKey(Interlocked.Increment(ref _expiringHitOffset));

    /// <summary>
    /// Gets the next deterministic hit key.
    /// </summary>
    /// <returns>A key from the hit keyspace.</returns>
    protected string NextHitKey() => Scenario.Topology == BenchmarkTopology.TwoNodeHotKeys
        ? Keyspace.HotKey(Interlocked.Increment(ref _hitOffset))
        : Keyspace.HitKey(Interlocked.Increment(ref _hitOffset));

    /// <summary>
    /// Gets the next deterministic miss key.
    /// </summary>
    /// <returns>A key from the miss keyspace.</returns>
    protected string NextMissKey() => Keyspace.MissKey(Interlocked.Increment(ref _missOffset));

    /// <summary>
    /// Gets the next globally unique add key for benchmark paths that require missing keys across all BenchmarkDotNet iterations.
    /// </summary>
    /// <returns>A key that has not been returned by this benchmark instance before.</returns>
    protected string NextUniqueAddKey() => string.Concat("unique:add:", Interlocked.Increment(ref _uniqueAddOffset).ToString("D10", CultureInfo.InvariantCulture));

    /// <summary>
    /// Allows derived benchmark classes to seed state that is specific to their pure-operation benchmark methods.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when additional setup is finished.</returns>
    protected virtual Task SeedAdditionalStateAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
