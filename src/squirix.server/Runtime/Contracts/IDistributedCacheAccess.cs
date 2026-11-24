using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Executes routed distributed cache access at the key owner.</summary>
internal interface IDistributedCacheAccess
{
    /// <summary>Reads or refreshes a value at its owner.</summary>
    /// <param name="operationId">The operation identifier for Refresh on a replicated owner.</param>
    /// <param name="cacheName">The validated cache name.</param>
    /// <param name="key">The validated key.</param>
    /// <param name="refresh">Whether to omit the value from the response.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The observed payload, including a zero-length value, or null for a miss or Refresh.</returns>
    ValueTask<ReadOnlyMemory<byte>?> AccessAsync(string operationId, string cacheName, string key, bool refresh, CancellationToken cancellationToken);
}
