using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Executes routed distributed cache access at the key owner.</summary>
internal interface IDistributedCacheAccess
{
    /// <summary>Reads or refreshes a value, replaying a retained response before observing current state.</summary>
    /// <param name="operationId">The validated operation identifier.</param>
    /// <param name="cacheName">The validated cache name.</param>
    /// <param name="key">The validated key.</param>
    /// <param name="fingerprintFactory">Builds the stable fingerprint only when single-copy replay or renewal requires it.</param>
    /// <param name="refresh">Whether to omit the value from the response.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The observed value for Get, or an empty response for Refresh.</returns>
    Task<GetDistributedAsyncResponse> AccessAsync(string operationId, string cacheName, string key, Func<string, string, bool, string> fingerprintFactory, bool refresh, CancellationToken cancellationToken);
}
