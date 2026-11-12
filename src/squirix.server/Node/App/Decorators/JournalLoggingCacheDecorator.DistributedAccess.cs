using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.App.Decorators;

internal sealed partial class JournalLoggingCacheDecorator<T>
{
    /// <summary>Gets the adapter operations that share this cache's write serialization.</summary>
    internal DistributedAccessOperations DistributedAccess { get; }

    /// <summary>Executes adapter renewal using the cache owner's synchronization and storage.</summary>
    [Immutable]
    internal sealed class DistributedAccessOperations
    {
        private readonly JournalLoggingCacheDecorator<T> _owner;

        internal DistributedAccessOperations(JournalLoggingCacheDecorator<T> owner)
        {
            _owner = owner;
        }

        /// <summary>Reads and renews the same entry under the journal's key lock and apply slot.</summary>
        /// <param name="operationId">Client operation identifier.</param>
        /// <param name="cacheName">Target cache name.</param>
        /// <param name="key">Target key.</param>
        /// <param name="refresh">Whether to omit the payload from the result.</param>
        /// <param name="cancellationToken">Cancellation token before the journal append.</param>
        /// <returns>The original payload when requested and present.</returns>
        internal async ValueTask<ReadOnlyMemory<byte>?> AccessDistributedAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken)
        {
            using var upsert = new DecidedUpsert();
            var state = new DistributedAccessState(_owner, operationId, cacheName, key, refresh, upsert);
            var result = await _owner._executor.ExecuteAsync(
                new CacheKey(cacheName, key),
                static (s, ct) => PrepareDistributedAccessAsync(s, ct),
                new DurableMutationPipeline<DistributedAccessState, ValueTuple<ReadOnlyMemory<byte>?>>(
                    state,
                    static (s, ownership, ct) => s.Self._journal.AppendPutAsync(ownership, new CacheKey(s.CacheName, s.Key), s.Upsert.Payload, ct),
                    static (s, ct) => ApplyDistributedAccessAsync(s, ct),
                    static (_, predicted) => RpcMutationIdempotencyExecutionAmbient.AppendPredictedOutcomeAsync(predicted.Item1),
                    static (_, _) => RpcMutationIdempotencyExecutionAmbient.PromoteOutcomeAfterApply()),
                cancellationToken).ConfigureAwait(false);
            return result.Item1;
        }

        private static async ValueTask<DurableMutationCondition<ValueTuple<ReadOnlyMemory<byte>?>>> PrepareDistributedAccessAsync(DistributedAccessState state, CancellationToken cancellationToken)
        {
            var current = await state.Self.ReadCurrentAsync(state.CacheName, state.Key, cancellationToken).ConfigureAwait(false);
            var (payload, renewTo, _) = DistributedCacheAccess.Decide(current, state.Self.UtcNow);

            // The executor reserves a null skip result for a missing decision.
            var result = ValueTuple.Create(state.Refresh ? null : payload);
            if (renewTo is not { } deadline)
                return DurableMutationCondition<ValueTuple<ReadOnlyMemory<byte>?>>.Skip(result);

            var entry = new NodeCacheEntry<T>(current!.Value, current.Version, deadline, tags: current.Tags);
            state.Upsert.Set(entry, EncodeWhole(entry));
            return DurableMutationCondition<ValueTuple<ReadOnlyMemory<byte>?>>.Apply(result);
        }

        private static async ValueTask<ValueTuple<ReadOnlyMemory<byte>?>> ApplyDistributedAccessAsync(DistributedAccessState state, CancellationToken cancellationToken)
        {
            var entry = state.Upsert.Entry ?? ThrowHelper.Throw<NodeCacheEntry<T>>(new InvalidOperationException("The distributed renewal was not prepared."));
            await state.Self._inner.SetEntryAsync(state.OperationId, state.CacheName, state.Key, entry, state.Upsert.Payload.Length, cancellationToken).ConfigureAwait(false);
            return ValueTuple.Create(state.Refresh ? default(ReadOnlyMemory<byte>?) : DistributedCacheAccess.ReadPayload(entry.Value, out _));
        }

        [Immutable]
        private readonly record struct DistributedAccessState(
            JournalLoggingCacheDecorator<T> Self,
            string OperationId,
            string CacheName,
            string Key,
            bool Refresh,
            DecidedUpsert Upsert);
    }
}
