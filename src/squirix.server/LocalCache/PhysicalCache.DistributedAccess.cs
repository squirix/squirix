using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

internal sealed partial class PhysicalCache<T>
{
    /// <summary>Gets the adapter operations that share this cache's write serialization.</summary>
    internal DistributedAccessOperations DistributedAccess { get; }

    /// <summary>Executes adapter renewal using the cache owner's synchronization and storage.</summary>
    [Immutable]
    internal sealed class DistributedAccessOperations
    {
        private readonly PhysicalCache<T> _owner;

        internal DistributedAccessOperations(PhysicalCache<T> owner)
        {
            _owner = owner;
        }

        /// <summary>Checks whether an observed live entry already covers the owner's current sliding deadline.</summary>
        /// <param name="entry">The immutable entry observed by the read path.</param>
        /// <returns>Whether the access can return without renewing an unexpired entry.</returns>
        internal bool CanReadWithoutRenewal(NodeCacheEntry<T> entry)
        {
            var (_, renewTo, expired) = DistributedCacheAccess.Decide(entry, _owner.UtcNow);
            return !expired && renewTo == null;
        }

        /// <summary>Observes and renews an adapter value under the same lock as local writes and removals.</summary>
        /// <param name="key">The adapter key.</param>
        /// <param name="refresh">Whether to omit the value from the result.</param>
        /// <param name="cancellationToken">Cancellation token before taking the cache lock.</param>
        /// <returns>The observed payload when requested and present.</returns>
        internal ReadOnlyMemory<byte>? AccessDistributed(CacheKey key, bool refresh, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_owner._lock)
            {
                if (!_owner.TryGetLiveLocked(key, false, out var node))
                    return null;

                var (payload, renewTo, expired) = DistributedCacheAccess.Decide(new NodeCacheEntry<T>(node.Value, expiresUtc: node.ExpiresUtc), _owner.UtcNow);
                if (expired)
                {
                    _ = _owner.RemoveLocked(key);
                    return null;
                }

                if (renewTo is { } deadline)
                {
                    var previous = node.ExpiresUtc;
                    node.WriteExpiresUtc(deadline);
                    _owner._deadlines.MovedLocked(node, previous);
                }

                _owner.TouchOrderLocked(key, node);
                return refresh ? null : payload;
            }
        }
    }
}
