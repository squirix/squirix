using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocation cost of one pass of the apply loop of a follower group whose log holds nothing to apply, woken by the apply signal.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class ReplicaApplyPassBenchmarks
{
    private const string GroupId = "n1";
    private const int PassesPerInvoke = 2_000;
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private ReplicaGroupCommitters? _committers;
    private CountingClock? _clock;
    private TempDirectory? _dir;
    private JournalBenchmarkHost? _host;
    private Meter? _meter;
    private ReplicaGroupRegistry? _registry;
    private ReplicaApplyService? _service;

    /// <summary>Stops the service and disposes the journal, the registry and the data directory.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_service != null)
        {
            await _service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            _service.Dispose();
        }

        if (_committers != null)
            await _committers.DisposeAsync().ConfigureAwait(false);
        if (_registry != null)
            await _registry.DisposeAsync().ConfigureAwait(false);
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _meter?.Dispose();
        _dir?.Dispose();
        _service = null;
        _committers = null;
        _registry = null;
        _host = null;
        _meter = null;
        _dir = null;
    }

    /// <summary>Wakes the apply loop and waits until it ran its pass and parked again.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = PassesPerInvoke)]
    public void WakePass()
    {
        var registry = ThrowHelper.Required(_registry, "Benchmark registry was not initialized.");
        var clock = ThrowHelper.Required(_clock, "Benchmark clock was not initialized.");
        var signal = registry.ApplySignalFor(GroupId);
        for (var i = 0; i < PassesPerInvoke; i++)
        {
            var parked = clock.Timers + 1;
            signal.Notify();
            while (clock.Timers < parked)
                Thread.SpinWait(20);
        }
    }

    /// <summary>Creates the apply service over an empty follower log and starts it.</summary>
    /// <returns>A task that completes when the loop is parked.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("replica-apply-pass-bench");
        await SeededLogs.SeedGroupsAsync(_dir, [GroupId], CancellationToken.None).ConfigureAwait(false);
        var logOptions = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        _registry = new ReplicaGroupRegistry(_dir, [GroupId], 3, Fingerprint, 1, NullLoggerFactory.Instance, logOptions) { ElectionClock = TimeProvider.System };
        await _registry.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        _host = await JournalBenchmarkHost.CreateAsync("replica-apply-pass-journal", new PersistenceOptions { JournalMaxSegmentMb = 64 }, CancellationToken.None)
            .ConfigureAwait(false);
        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        _meter = new Meter("replica-apply-pass-bench");
        var appliers = new ReplicaGroupAppliers(_registry, new ClientCache<object?>(physical, physical), GroupId, NullLogger<ReplicaGroupAppliers>.Instance, new ReplicationMetrics(_meter));
        _committers = new ReplicaGroupCommitters([], "n9", new SingleGroupLocator(), TimeProvider.System);
        _clock = new CountingClock();
        _service = new ReplicaApplyService(_registry, appliers, _committers, _host.Coordinator, NullLogger<ReplicaApplyService>.Instance, _clock);
        await _service.StartAsync(CancellationToken.None).ConfigureAwait(false);
        while (_clock.Timers < 1)
            await Task.Yield();
    }

    /// <summary>A clock that counts the timers created on it, one for each wait of the loop.</summary>
    [ThreadSafe]
    private sealed class CountingClock : TimeProvider
    {
        private long _timers;

        internal long Timers => Interlocked.Read(ref _timers);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = Interlocked.Increment(ref _timers);
            return base.CreateTimer(callback, state, dueTime, period);
        }
    }

    /// <summary>Assigns every key to the group of the benchmark.</summary>
    [Immutable]
    private sealed class SingleGroupLocator : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => GroupId;
    }
}
