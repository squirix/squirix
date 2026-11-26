using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Covers the context of retained and new replica applies after an earlier apply fails.</summary>
[Immutable]
public sealed class ReplicaPendingApplyScopeTests : IsolatedStorageTestBase
{
    private const string CallerOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Both the retained entry and its successor apply with replica durability active.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainedAndNewEntriesUseReplicaContext(CancellationToken cancellationToken)
    {
        var local = new ScopeRecordingCache();
        await using var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(Dir, cancellationToken);
        await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), local, NullLogger<ReplicaGroupCommitter>.Instance);
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(ReplicaOwnerTestKit.NewOperationId(), "cache", "k1", ReplicaOwnerTestKit.Entry("k1"), cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        await committer.CommitSetAsync(CallerOperationId, "cache", "k2", ReplicaOwnerTestKit.Entry("k2"), cancellationToken);

        await SequenceAssert.EqualAsync(["k1", "k2"], local.AppliedKeys(), StringComparer.Ordinal);
        _ = await Assert.That(local.AppliedInReplicaContext("k1")).IsTrue();
        _ = await Assert.That(local.AppliedInReplicaContext("k2")).IsTrue();
    }

    /// <summary>Records the apply context after an injected failure on the first write.</summary>
    [ThreadSafe]
    private sealed class ScopeRecordingCache : ILogicalNamespacedCache<StoredValue>
    {
        private readonly ConcurrentQueue<(string Key, bool ReplicaApply)> _applied = new();
        private int _failNext = 1;

        public ValueTask<NodeCacheEntry<StoredValue>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult<NodeCacheEntry<StoredValue>?>(null);

        public ValueTask<NodeCacheValueResult<StoredValue>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<StoredValue>(false, default));

        public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _failNext, 0) == 1)
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));

            _applied.Enqueue((key, ReplicaApplyContext.IsActive));
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, StoredValue value, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        internal string[] AppliedKeys()
        {
            var applied = _applied.ToArray();
            var keys = new string[applied.Length];
            for (var i = 0; i < applied.Length; i++)
                keys[i] = applied[i].Key;

            return keys;
        }

        internal bool AppliedInReplicaContext(string key)
        {
            foreach (var (applied, replicaApply) in _applied)
            {
                if (string.Equals(applied, key, StringComparison.Ordinal))
                    return replicaApply;
            }

            throw new InvalidOperationException($"Key '{key}' was never applied.");
        }
    }
}
