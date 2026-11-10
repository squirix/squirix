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
    /// <param name="fingerprint">The stable single-copy request fingerprint.</param>
    /// <param name="refresh">Whether to omit the value from the response.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The observed value for Get, or an empty response for Refresh.</returns>
    Task<GetDistributedAsyncResponse> AccessAsync(string operationId, string cacheName, string key, string fingerprint, bool refresh, CancellationToken cancellationToken);
}
