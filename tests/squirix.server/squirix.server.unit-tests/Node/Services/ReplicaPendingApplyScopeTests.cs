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
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An RF=3 group owner whose first write fails its memory apply after the majority: the next write, running under an RPC idempotency
/// scope, applies the retained entry before its own.
/// </summary>
[Immutable]
public sealed class ReplicaPendingApplyScopeTests : IsolatedStorageTestBase
{
    private const string CallerOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>
    /// The retained entry applied on behalf of a later idempotent RPC runs outside that RPC's
    /// <see cref="RpcMutationIdempotencyExecutionAmbient" /> scope, so its cache-WAL frame is not stamped with the foreign operation id;
    /// the later RPC's own entry is applied inside its scope with stamping suspended, because the group log is its durable source.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicatedEntriesAreNeverStamped(CancellationToken cancellationToken)
    {
        var local = new ScopeRecordingCache();
        await using var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(Dir, cancellationToken);
        await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), local);
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(
            committer.CommitSetAsync(ReplicaOwnerTestKit.NewOperationId(), "cache", "k1", ReplicaOwnerTestKit.Entry("k1"), cancellationToken));
        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        var scope = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, CallerOperationId, "caller-fingerprint");
        try
        {
            await committer.CommitSetAsync(CallerOperationId, "cache", "k2", ReplicaOwnerTestKit.Entry("k2"), cancellationToken);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        }

        await SequenceAssert.EqualAsync(["k1", "k2"], local.AppliedKeys(), StringComparer.Ordinal);
        _ = await Assert.That(local.ScopeFor("k1")).IsNull();
        _ = await Assert.That(local.ScopeFor("k2")).IsNull();
        _ = await Assert.That(local.StampingSuspendedFor("k2")).IsTrue();
    }

    /// <summary>
    /// Local cache double whose first replicated write fails after the majority; it records every applied key with the RPC idempotency
    /// operation id active while it was applied.
    /// </summary>
    [ThreadSafe]
    private sealed class ScopeRecordingCache : ILogicalNamespacedCache<object?>
    {
        private readonly ConcurrentQueue<(string Key, string? OperationId, bool StampingSuspended)> _applied = new();
        private int _failNext = 1;

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult<NodeCacheEntry<object?>?>(null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _failNext, 0) == 1)
                return ValueTask.FromException(new InvalidOperationException("Injected memory apply failure after the majority."));

            _applied.Enqueue((key, RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue, RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended));
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        internal string[] AppliedKeys()
        {
            var applied = _applied.ToArray();
            var keys = new string[applied.Length];
            for (var i = 0; i < applied.Length; i++)
                keys[i] = applied[i].Key;

            return keys;
        }

        internal bool StampingSuspendedFor(string key)
        {
            foreach (var (applied, _, suspended) in _applied)
            {
                if (string.Equals(applied, key, StringComparison.Ordinal))
                    return suspended;
            }

            throw new InvalidOperationException($"Key '{key}' was never applied.");
        }

        internal string? ScopeFor(string key)
        {
            foreach (var (applied, operationId, _) in _applied)
            {
                if (string.Equals(applied, key, StringComparison.Ordinal))
                    return operationId;
            }

            throw new InvalidOperationException($"Key '{key}' was never applied.");
        }
    }
}
