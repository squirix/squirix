using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Shared harness of the batch expiry tests: a leader of group n1 over a committed-records cache, and the keys and log reads they use.</summary>
internal static class ReplicaExpiryKit
{
    internal const string CacheName = "cache";

    internal static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    internal static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromSeconds(5);

    internal static (string CacheName, string Key)[] KeysOf(int count)
    {
        var keys = new (string CacheName, string Key)[count];
        for (var i = 0; i < count; i++)
            keys[i] = (CacheName, "key-" + i.ToString(CultureInfo.InvariantCulture));

        return keys;
    }

    internal static string[] NamesOf((string CacheName, string Key)[] keys)
    {
        var names = new string[keys.Length];
        for (var i = 0; i < keys.Length; i++)
            names[i] = keys[i].Key;

        return names;
    }

    /// <summary>Writes the keys with a time to live and moves the clock past it, so every one of them is expired.</summary>
    /// <param name="leader">The leader.</param>
    /// <param name="clock">The decision clock of the leader.</param>
    /// <param name="keys">The keys to write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task SeedExpiredAsync(Leader leader, FakeTimeProvider clock, string[] keys, CancellationToken cancellationToken)
    {
        foreach (var key in keys)
            await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, key, new NodeCacheEntry<object?>(key, 1, null, Ttl), cancellationToken);

        clock.Advance(Ttl);
    }

    internal static async Task<int> CountExpireRecordsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var records = await RecordsAsync(registry, cancellationToken);
        var count = 0;
        for (var i = 0; i < records.Count; i++)
            count += string.Equals(records[i].MutationKind, ReplicaMutationKinds.Expire, StringComparison.Ordinal) ? 1 : 0;

        return count;
    }

    internal static async Task<List<ReplicaLogRecord>> RecordsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var read = await log.ReadEntriesAsync(1, 256, cancellationToken);
        var records = new List<ReplicaLogRecord>(read.Entries.Count);
        foreach (var entry in read.Entries)
            records.Add(ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode.")));

        return records;
    }

    /// <summary>The owner of group n1 over an open registry: a committed-records cache and a committer on one fake clock.</summary>
    internal sealed class Leader : IAsyncDisposable
    {
        internal Leader(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, FakeTimeProvider clock, bool budgetOnClock = false)
        {
            Physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
            var local = new ClientCache<object?>(Physical, Physical);
            Committer = budgetOnClock
                ? CreateCommitter(registry, gateway, local, clock, NullLogger<ReplicaGroupCommitter>.Instance, (TimeProvider.System, ShutdownBudget))
                : CreateCommitter(registry, gateway, local, clock);
        }

        internal ReplicaGroupCommitter Committer { get; }

        internal PhysicalCache<object?> Physical { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();

        internal ValueTask<NodeCacheEntry<object?>?> RawAsync(string key, CancellationToken cancellationToken) =>
            Physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, key), cancellationToken);
    }
}
