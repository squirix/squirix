using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Helpers shared by the tests of the commit batches of a replica group: an RF=2 owner over scripted followers.</summary>
internal static class ReplicaCommitBatchKit
{
    internal const string Cache = "cache";
    internal const string OwnedGroup = "n1";

    internal static readonly TimeSpan SmallBudget = TimeSpan.FromSeconds(1);

    internal static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly byte[] Fingerprint = [9, 8, 7];

    internal static string Applied(ScriptedApplyCache local) => string.Join(',', local.Applied);

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ScriptedApplyCache local, IReplicaRpcGateway gateway, TimeProvider budgetTime) =>
        CreateCommitter(registry, local, gateway, (budgetTime, null));

    internal static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ScriptedApplyCache local, IReplicaRpcGateway gateway, (TimeProvider BudgetTime, ReplicationMetrics? Metrics) options) =>
        new(registry, new TwoNodeLocator(), gateway, local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Clock = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = options.BudgetTime,
            Metrics = options.Metrics,
            Recovery = RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, OwnedGroup, OwnedGroup),
            CommitBudget = SmallBudget,
        };

    internal static FollowerLogOptions DefaultOptions() => new() { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };

    internal static FollowerLogOptions DefaultOptions(int idempotencyCapacity) =>
        new() { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System, IdempotencyCapacity = idempotencyCapacity };

    internal static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    internal static FollowerLog GetOwnedLog(ReplicaGroupRegistry registry) =>
        registry.TryGetLog(OwnedGroup, out var log) && log is FollowerLog followerLog ? followerLog : throw new InvalidOperationException("The owned group log is not open.");

    internal static string NewOperationId() => Guid.NewGuid().ToString("N");

    internal static async Task<ReplicaGroupRegistry> OpenRegistryAsync(TempDirectory dir, FollowerLogOptions options, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, [OwnedGroup], cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, [OwnedGroup], 2, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
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

    internal static Task<bool> PrepareWriteAsync(
        ReplicaGroupCommitter committer,
        string key,
        Func<ReplicaMutationFactory, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> prepare,
        CancellationToken cancellationToken) => committer.CommitAsync(
        (Cache, NewOperationId(), key),
        prepare,
        static _ => Fingerprint,
        static (factory, state, index, token) => state(factory, index, token),
        static _ => new ValueTask<bool>(true),
        cancellationToken);

    internal static Task[] QueueSets(ReplicaGroupCommitter committer, int count, CancellationToken cancellationToken)
    {
        var writes = new Task[count];
        for (var i = 0; i < writes.Length; i++)
            writes[i] = SetAsync(committer, "k" + int.CreateChecked(i + 1).ToString(System.Globalization.CultureInfo.InvariantCulture), cancellationToken);

        return writes;
    }

    internal static ScriptedApplyCache RecoveredCache() => RecoveredCache(int.MaxValue);

    internal static ScriptedApplyCache RecoveredCache(int failFromApply)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail) { FailFromApply = failFromApply };
        local.Recover();
        return local;
    }

    internal static Task SetAsync(ReplicaGroupCommitter committer, string key, CancellationToken cancellationToken) => SetAsync(committer, key, NewOperationId(), cancellationToken);

    internal static Task SetAsync(ReplicaGroupCommitter committer, string key, string operationId, CancellationToken cancellationToken) =>
        committer.CommitSetAsync(operationId, Cache, key, Entry(), cancellationToken);
}
