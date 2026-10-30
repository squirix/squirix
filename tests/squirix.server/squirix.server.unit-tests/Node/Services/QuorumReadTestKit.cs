using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Harness of the quorum-read tests: node n1 leads group n2 by election in term 2 under quorum reads, with followers n2 and n3.</summary>
internal static class QuorumReadTestKit
{
    internal const string CacheName = "cache";

    internal static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    internal static readonly ElectionTimerOptions ElectionTiming = new() { JitterSeed = 1UL };

    /// <summary>Creates the memory admission decorator the host builds over a replicated cache, under a pressure gate with a large limit.</summary>
    /// <param name="replicated">The replicated cache below admission.</param>
    /// <param name="meter">The meter of the pressure gate.</param>
    /// <returns>The decorator.</returns>
    internal static MemoryAdmissionCacheDecorator<object?> CreateAdmission(ReplicatedCache replicated, Meter meter)
    {
        var estimator = new CacheEntrySizeEstimator<object?>();
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options.Value), new MemoryUsageAccounting(new PhysicalCache<object?>(TimeProvider.System, estimator)), "n1", meter);
        return CachePipelineRegistration.CreateAdmissionDecorator(replicated, gate, estimator, static (_, _, _) => false);
    }

    /// <summary>Reads the last log index of the group n2 log.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The last log index.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    internal static async Task<ulong> LastLogIndexAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        registry.TryGetLog("n2", out var log) ? (await log.GetStatusAsync(cancellationToken)).LastLogIndex : throw new InvalidOperationException("The group log n2 is not open.");

    /// <summary>Promotes node n1 to lead group n2 in term 2 and grants it authority, as the election driver does once the leader-term entry commits.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="committers">The committers of node n1.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The committer of group n2.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open, or the promotion was not authorized.</exception>
    internal static async Task<ReplicaGroupCommitter> AuthorizeAsync(ReplicaGroupRegistry registry, ReplicaGroupCommitters committers, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        var authorized = await committers.PromoteAsync("n2", 2UL, cancellationToken) && state.BecomeLeader(2UL) && state.GrantAuthority(2UL);
        return authorized ? committers.For("n2") : throw new InvalidOperationException("Node n1 must lead group n2 with authority in term 2.");
    }

    internal static ReplicaGroupCommitters Lead(ReplicaGroupRegistry registry, ScriptedGateway gateway, StubCache cache, ReplicaGroupApplier? applier = null) =>
        new(
            groupId => CreateElectedCommitter(registry, groupId, gateway, cache, applier ?? new ReplicaGroupApplier(cache, NullLogger.Instance, groupId, "n1")),
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);

    internal static async Task<ReplicaGroupRegistry> OpenAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, Groups, cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1UL, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System })
        {
            Election = ElectionTiming,
            ElectionClock = time,
            QuorumReads = true,
        };

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
}
